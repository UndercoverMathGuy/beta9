"""
Dependency discovery for Python projects.

This module provides automatic detection of Python package dependencies by analyzing
project files. It uses a multi-phase approach with strict priority ordering to ensure
accurate and trustworthy dependency resolution.

Resolution Priority:
    1. Lockfiles (uv.lock, poetry.lock, Pipfile.lock) - Fully trusted with pinned versions
    2. Manifests (pyproject.toml, requirements.txt, etc.) - Trusted for package names
    3. AST scanning - Discovers imports not in manifests, uses importlib.metadata for versions

Example:
    Basic usage to discover dependencies from a project:

    ```python
    from containerizer.dependencies import discover_dependencies

    deps = discover_dependencies("./my_project")
    print(deps.to_pip_specs())  # ["requests>=2.28.0", "flask==2.3.0", ...]
    ```

    Scoped discovery for monorepo subdirectory:

    ```python
    deps = discover_dependencies("./monorepo", target="packages/api")
    ```
"""
import functools
import pathlib as pl
import sys
from typing import Optional, Union

from .utils.file_load import _discover_local_toplevel_modules, DEFAULT_EXCLUDE_DIRS
from .utils.classes import (
    DependencySpec,
    UnresolvedPackage,
    ProjectDependencies,
)
from .utils.package_name import resolve_import_to_package
from .utils.manifest_parser import (
    parse_pyproject_toml,
    parse_requirements_txt,
    parse_setup_py,
    parse_setup_cfg,
    parse_pipfile,
    parse_pipfile_lock,
    parse_poetry_lock,
    parse_uv_lockfile,
    parse_dep_spec,
)
from .utils.ast_scanner import (
    extract_imports_from_file,
    extract_pep723_deps,
)
from .utils.monorepo import detect_monorepo
import re


def _normalize_package_name(name: str) -> str:
    """
    Normalize a package name per PEP 503.
    
    Converts to lowercase and replaces underscores, dots, and multiple hyphens
    with single hyphens.
    """
    return re.sub(r"[-_.]+", "-", name).lower()


def _find_repo_root(start_path: pl.Path) -> pl.Path:
    """
    Find the repository root by looking for marker files.

    Traverses up the directory tree from the starting path until it finds a directory
    containing one of the marker files (pyproject.toml, setup.py, setup.cfg, .git, or
    requirements.txt).

    Parameters:
        start_path (pl.Path):
            The starting path for the search. Can be a file or directory.

    Returns:
        pl.Path: The repository root path. If no marker is found, returns the starting
            directory or its parent if start_path is a file.
    """
    current = start_path if start_path.is_dir() else start_path.parent
    markers = ["pyproject.toml", "setup.py", "setup.cfg", ".git", "requirements.txt"]

    while current != current.parent:
        for marker in markers:
            if (current / marker).exists():
                return current
        current = current.parent

    return start_path if start_path.is_dir() else start_path.parent


def _iter_python_files(
    repo_root: pl.Path, target_dir: Optional[pl.Path] = None
) -> list[pl.Path]:
    """
    Iterate all Python files in a repository or target directory.

    Recursively finds all .py files while excluding common non-source directories
    like .git, __pycache__, venv, node_modules, and site-packages.

    Parameters:
        repo_root (pl.Path):
            The repository root path used for relative path calculations.
        target_dir (Optional[pl.Path]):
            Optional subdirectory to scope the search. If None, searches from repo_root.

    Returns:
        list[pl.Path]: List of paths to Python files.
    """
    search_root = target_dir if target_dir else repo_root
    py_files = []
    # MEDIUM FIX #28: Track visited real paths to prevent symlink loops
    visited_real_paths: set = set()

    for path in search_root.rglob("*.py"):
        # Prevent symlink-based DoS by checking real paths
        try:
            real_path = path.resolve()
            if real_path in visited_real_paths:
                continue
            visited_real_paths.add(real_path)

            # Skip symlinks that point outside the repo
            if path.is_symlink():
                try:
                    real_path.relative_to(repo_root.resolve())
                except ValueError:
                    # Symlink points outside repo - skip
                    continue

            parts = path.relative_to(repo_root).parts
        except ValueError:
            parts = path.parts
        except OSError:
            # Handle broken symlinks or permission errors
            continue

        if any(part in DEFAULT_EXCLUDE_DIRS or part.startswith(".") for part in parts):
            continue
        if any(p in parts for p in ("site-packages", "dist-packages")):
            continue
        py_files.append(path)
    return py_files


# Fallback stdlib modules for Python < 3.10
_FALLBACK_STDLIB = {
    "abc", "aifc", "argparse", "array", "ast", "asynchat", "asyncio", "asyncore",
    "atexit", "audioop", "base64", "bdb", "binascii", "binhex", "bisect", "builtins",
    "bz2", "calendar", "cgi", "cgitb", "chunk", "cmath", "cmd", "code", "codecs",
    "codeop", "collections", "colorsys", "compileall", "concurrent", "configparser",
    "contextlib", "contextvars", "copy", "copyreg", "cProfile", "crypt", "csv",
    "ctypes", "curses", "dataclasses", "datetime", "dbm", "decimal", "difflib",
    "dis", "distutils", "doctest", "email", "encodings", "enum", "errno", "faulthandler",
    "fcntl", "filecmp", "fileinput", "fnmatch", "fractions", "ftplib", "functools",
    "gc", "getopt", "getpass", "gettext", "glob", "graphlib", "grp", "gzip", "hashlib",
    "heapq", "hmac", "html", "http", "idlelib", "imaplib", "imghdr", "imp", "importlib",
    "inspect", "io", "ipaddress", "itertools", "json", "keyword", "lib2to3", "linecache",
    "locale", "logging", "lzma", "mailbox", "mailcap", "marshal", "math", "mimetypes",
    "mmap", "modulefinder", "multiprocessing", "netrc", "nis", "nntplib", "numbers",
    "operator", "optparse", "os", "ossaudiodev", "pathlib", "pdb", "pickle", "pickletools",
    "pipes", "pkgutil", "platform", "plistlib", "poplib", "posix", "posixpath", "pprint",
    "profile", "pstats", "pty", "pwd", "py_compile", "pyclbr", "pydoc", "queue", "quopri",
    "random", "re", "readline", "reprlib", "resource", "rlcompleter", "runpy", "sched",
    "secrets", "select", "selectors", "shelve", "shlex", "shutil", "signal", "site",
    "smtpd", "smtplib", "sndhdr", "socket", "socketserver", "spwd", "sqlite3", "ssl",
    "stat", "statistics", "string", "stringprep", "struct", "subprocess", "sunau",
    "symtable", "sys", "sysconfig", "syslog", "tabnanny", "tarfile", "telnetlib", "tempfile",
    "termios", "test", "textwrap", "threading", "time", "timeit", "tkinter", "token",
    "tokenize", "trace", "traceback", "tracemalloc", "tty", "turtle", "turtledemo",
    "types", "typing", "unicodedata", "unittest", "urllib", "uu", "uuid", "venv",
    "warnings", "wave", "weakref", "webbrowser", "winreg", "winsound", "wsgiref",
    "xdrlib", "xml", "xmlrpc", "zipapp", "zipfile", "zipimport", "zlib", "_thread",
}


@functools.lru_cache(maxsize=1)
def _get_stdlib_modules() -> set[str]:
    """
    Get the set of Python standard library module names.

    Uses sys.stdlib_module_names (Python 3.10+) to get an accurate list of stdlib modules.
    Falls back to a hardcoded set for older Python versions.

    Returns:
        set[str]: Set of lowercase stdlib module names.
    """
    if hasattr(sys, "stdlib_module_names"):
        return {m.lower() for m in sys.stdlib_module_names}
    return {m.lower() for m in _FALLBACK_STDLIB}


def _is_stdlib(module_name: str) -> bool:
    """
    Check if a module is part of the Python standard library.

    Parameters:
        module_name (str):
            The full module name (e.g., "os.path", "json").

    Returns:
        bool: True if the module is in the standard library, False otherwise.
    """
    if not module_name:
        return False
    top_level = module_name.split(".")[0].lower()
    stdlib_mods = _get_stdlib_modules()
    return top_level in stdlib_mods if stdlib_mods else False


def discover_dependencies(
    project_path: Union[str, pl.Path],
    target: Optional[Union[str, pl.Path]] = None,
) -> ProjectDependencies:
    """
    Discover Python package dependencies for a project.

    Analyzes a Python project to automatically detect its dependencies using a
    multi-phase approach. Dependencies are discovered from lockfiles, manifest files,
    and source code analysis (AST scanning).

    Resolution priority (highest to lowest trust):
        1. Lockfiles (uv.lock, poetry.lock, Pipfile.lock) - Fully trusted with pinned versions
        2. Manifests (pyproject.toml, requirements.txt, setup.py, setup.cfg, Pipfile)
        3. AST scanning - Discovers imports not in manifests

    Parameters:
        project_path (Union[str, pl.Path]):
            Path to the project root directory. The function will search for the
            repository root by looking for marker files (pyproject.toml, setup.py, etc.).
        target (Optional[Union[str, pl.Path]]):
            Optional subdirectory to scope dependency discovery. Useful for monorepos
            where you want to analyze only a specific package. Default is None.

    Returns:
        ProjectDependencies: A dataclass containing:
            - python_version: Detected Python version requirement
            - third_party: Dict of package name to DependencySpec
            - stdlib: Set of detected standard library imports
            - local: Set of detected local package imports
            - has_lockfile: Whether a lockfile was found
            - unresolved: List of imports that couldn't be resolved

    Raises:
        ValueError: If the target directory does not exist or is not a directory.

    Example:
        Basic usage:

        ```python
        deps = discover_dependencies("./my_project")

        # Get pip-installable specs
        pip_specs = deps.to_pip_specs()
        # ["requests>=2.28.0", "flask==2.3.0", "numpy"]

        # Check if lockfile was used
        if deps.has_lockfile:
            print("Using pinned versions from lockfile")

        # Handle unresolved imports
        for unresolved in deps.unresolved:
            print(f"Could not resolve: {unresolved.import_name}")
        ```

        Monorepo usage:

        ```python
        # Analyze only the API package in a monorepo
        deps = discover_dependencies("./monorepo", target="packages/api")
        ```
    """
    path = pl.Path(project_path)
    repo_root = _find_repo_root(path)

    # Resolve target directory
    target_dir = None
    if target:
        # CRITICAL FIX #6: Prevent path traversal - always resolve relative to repo_root
        # and validate the resolved path is within repo_root
        if pl.Path(target).is_absolute():
            target_dir = pl.Path(target).resolve()
        else:
            target_dir = (repo_root / target).resolve()

        # Security check: ensure target is within repo_root (prevent path traversal)
        try:
            target_dir.relative_to(repo_root.resolve())
        except ValueError:
            raise ValueError(
                f"Target directory '{target}' is outside repository root. "
                f"Path traversal is not allowed for security reasons."
            )

        if not target_dir.exists():
            raise ValueError(f"Target directory does not exist: {target_dir}")
        if not target_dir.is_dir():
            raise ValueError(f"Target must be a directory: {target_dir}")
    
    deps = ProjectDependencies()
    local_modules = _discover_local_toplevel_modules(repo_root)
    
    # Lockfiles
    
    lockfile_packages: dict[str, str] = {}
    
    # uv.lock
    uv_lock = repo_root / "uv.lock"
    if uv_lock.exists():
        deps.has_lockfile = True
        lockfile_packages.update(parse_uv_lockfile(uv_lock))
    
    # poetry.lock
    poetry_lock_versions = parse_poetry_lock(repo_root)
    if poetry_lock_versions:
        deps.has_lockfile = True
        for name, version in poetry_lock_versions.items():
            if name not in lockfile_packages:
                lockfile_packages[name] = version
    
    # Pipfile.lock
    pipfile_lock_versions = parse_pipfile_lock(repo_root)
    if pipfile_lock_versions:
        deps.has_lockfile = True
        for name, version in pipfile_lock_versions.items():
            if name not in lockfile_packages:
                lockfile_packages[name] = version
    
    # Add lockfile packages (normalized names to avoid duplicates)
    for name, version in lockfile_packages.items():
        normalized = _normalize_package_name(name)
        deps.third_party[normalized] = DependencySpec(
            name=name,
            version_spec=f"=={version}",
            source="lockfile",
        )
    
    # Manifests
    
    # pyproject.toml
    pyproject_deps, optional_deps, python_req, uv_sources = parse_pyproject_toml(repo_root)
    deps.python_version = python_req
    deps.uv_sources = uv_sources
    
    for dep_str in pyproject_deps:
        spec = parse_dep_spec(dep_str, source="pyproject")
        normalized = _normalize_package_name(spec.name)
        if normalized not in deps.third_party:
            deps.third_party[normalized] = spec
    
    for group, group_deps in optional_deps.items():
        for dep_str in group_deps:
            spec = parse_dep_spec(dep_str, source=f"pyproject[{group}]")
            normalized = _normalize_package_name(spec.name)
            if normalized not in deps.third_party:
                deps.third_party[normalized] = spec
    
    # setup.cfg
    for dep_str in parse_setup_cfg(repo_root):
        spec = parse_dep_spec(dep_str, source="setup.cfg")
        normalized = _normalize_package_name(spec.name)
        if normalized not in deps.third_party:
            deps.third_party[normalized] = spec
    
    # setup.py
    for dep_str in parse_setup_py(repo_root):
        spec = parse_dep_spec(dep_str, source="setup.py")
        normalized = _normalize_package_name(spec.name)
        if normalized not in deps.third_party:
            deps.third_party[normalized] = spec
    
    # requirements.txt
    for dep_str in parse_requirements_txt(repo_root):
        spec = parse_dep_spec(dep_str, source="requirements")
        normalized = _normalize_package_name(spec.name)
        if normalized not in deps.third_party:
            deps.third_party[normalized] = spec
    
    # Pipfile
    for dep_str in parse_pipfile(repo_root):
        spec = parse_dep_spec(dep_str, source="pipfile")
        normalized = _normalize_package_name(spec.name)
        if normalized not in deps.third_party:
            deps.third_party[normalized] = spec
    
    # Monorepo packages
    monorepo_pkgs = detect_monorepo(repo_root)
    if monorepo_pkgs:
        for mono_pkg in monorepo_pkgs:
            if target_dir:
                try:
                    target_dir.relative_to(mono_pkg.path)
                    is_relevant = True
                except ValueError:
                    try:
                        mono_pkg.path.relative_to(target_dir)
                        is_relevant = True
                    except ValueError:
                        is_relevant = False
                if not is_relevant:
                    continue
            
            sub_deps, _, _, _ = parse_pyproject_toml(mono_pkg.path)
            for dep_str in sub_deps:
                spec = parse_dep_spec(dep_str, source=f"monorepo:{mono_pkg.name}")
                normalized = _normalize_package_name(spec.name)
                if normalized not in deps.third_party:
                    deps.third_party[normalized] = spec
            
            for dep_str in parse_requirements_txt(mono_pkg.path):
                spec = parse_dep_spec(dep_str, source=f"monorepo:{mono_pkg.name}")
                normalized = _normalize_package_name(spec.name)
                if normalized not in deps.third_party:
                    deps.third_party[normalized] = spec
    
    # Target-specific manifests (don't override lockfile versions)
    if target_dir and target_dir != repo_root:
        target_deps, _, target_python, target_uv = parse_pyproject_toml(target_dir)
        for dep_str in target_deps:
            spec = parse_dep_spec(dep_str, source="target:pyproject")
            normalized = _normalize_package_name(spec.name)
            if normalized not in deps.third_party:
                deps.third_party[normalized] = spec
        
        for dep_str in parse_requirements_txt(target_dir):
            spec = parse_dep_spec(dep_str, source="target:requirements")
            normalized = _normalize_package_name(spec.name)
            if normalized not in deps.third_party:
                deps.third_party[normalized] = spec
        
        if target_python:
            deps.python_version = target_python
        if target_uv:
            deps.uv_sources.update(target_uv)
    
    # ═══════════════════════════════════════════════════════════════════════════
    # PHASE 3: AST SCANNING - Discover imports not in manifests
    # ═══════════════════════════════════════════════════════════════════════════
    
    for py_file in _iter_python_files(repo_root, target_dir):
        # PEP 723 inline script dependencies
        for dep_str in extract_pep723_deps(py_file):
            spec = parse_dep_spec(dep_str, source="pep723")
            normalized = _normalize_package_name(spec.name)
            if normalized not in deps.third_party:
                deps.third_party[normalized] = spec
        
        # AST import scanning
        for module_name, is_conditional in extract_imports_from_file(py_file):
            top_level = module_name.split(".")[0]
            
            if top_level.startswith("."):
                continue
            
            if _is_stdlib(top_level):
                deps.stdlib.add(top_level)
            elif top_level in local_modules:
                deps.local.add(top_level)
            else:
                pkg_name, pkg_version, is_resolved, method = resolve_import_to_package(top_level)
                normalized = _normalize_package_name(pkg_name.split("[")[0])
                
                if _normalize_package_name(top_level) in deps.third_party:
                    continue
                if normalized in deps.third_party:
                    continue
                
                if not is_resolved:
                    # Don't add unresolved imports to third_party - they're likely local modules
                    # Just track them as unresolved for debugging
                    deps.unresolved.append(UnresolvedPackage(
                        import_name=top_level,
                        guessed_package=pkg_name,
                        reason=f"Import '{top_level}' not found in manifests or installed packages",
                        suggestions=[
                            f"Add '{pkg_name}' to your requirements.txt or pyproject.toml",
                        ],
                    ))
                    continue
                
                version_spec = f"=={pkg_version}" if pkg_version else None
                deps.third_party[normalized] = DependencySpec(
                    name=pkg_name,
                    version_spec=version_spec,
                    source=f"ast:{method}",
                    is_conditional=is_conditional,
                )
    
    return deps


def get_pip_packages(
    project_path: Union[str, pl.Path],
    target: Optional[Union[str, pl.Path]] = None,
) -> list[str]:
    """
    Get a list of pip-installable package specifications for a project.

    This is a convenience function that wraps discover_dependencies() and returns
    only the pip-installable package specifications as strings.

    Parameters:
        project_path (Union[str, pl.Path]):
            Path to the project root directory.
        target (Optional[Union[str, pl.Path]]):
            Optional subdirectory to scope dependency discovery. Default is None.

    Returns:
        list[str]: List of pip package specifications ready for installation.
            Examples: ["requests>=2.28.0", "flask==2.3.0", "numpy"]

    Example:
        ```python
        packages = get_pip_packages("./my_project")
        # ["requests>=2.28.0", "flask==2.3.0", "numpy"]

        # Use with pip programmatically
        import subprocess
        subprocess.run(["pip", "install"] + packages)
        ```
    """
    deps = discover_dependencies(project_path, target)
    return [spec.to_pip_spec() for spec in deps.third_party.values()]
