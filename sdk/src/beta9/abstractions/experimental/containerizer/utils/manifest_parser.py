"""
Manifest file parsers for Python projects.

This module provides parsers for all common Python project manifest and lockfile formats.
It extracts dependency specifications from:

Manifest Files (declare dependencies):
    - pyproject.toml (PEP 621, Poetry)
    - requirements.txt (pip)
    - setup.py (setuptools)
    - setup.cfg (setuptools)
    - Pipfile (pipenv)

Lockfiles (pin exact versions):
    - uv.lock (uv)
    - poetry.lock (Poetry)
    - Pipfile.lock (pipenv)

Example:
    ```python
    from pathlib import Path
    from containerizer.utils.manifest_parser import (
        parse_pyproject_toml,
        parse_requirements_txt,
        parse_uv_lockfile,
    )

    repo = Path("./my_project")

    # Parse pyproject.toml
    deps, optional, python_req, uv_sources = parse_pyproject_toml(repo)
    print(deps)  # ["requests>=2.0", "flask"]

    # Parse lockfile for pinned versions
    versions = parse_uv_lockfile(repo / "uv.lock")
    print(versions)  # {"requests": "2.31.0", "flask": "3.0.0"}
    ```
"""
import ast
import configparser
import json
import pathlib as pl
import re
import sys
from typing import Dict, List, Optional, Set, Tuple

# tomllib is Python 3.11+, fallback to tomli for older versions
if sys.version_info >= (3, 11):
    import tomllib
else:
    try:
        import tomli as tomllib
    except ImportError:
        tomllib = None  # type: ignore

from .classes import DependencySpec


def parse_pyproject_toml(
    repo_root: pl.Path,
) -> Tuple[List[str], Dict[str, List[str]], Optional[str], Dict]:
    """
    Parse pyproject.toml for dependencies and project metadata.

    Extracts dependencies from PEP 621 format ([project] section) and also
    captures UV-specific source configurations for custom package indexes
    (e.g., PyTorch CUDA builds).

    Parameters:
        repo_root (pl.Path):
            Path to the repository root containing pyproject.toml.

    Returns:
        Tuple containing:
            - List[str]: Main dependencies from [project.dependencies]
            - Dict[str, List[str]]: Optional dependencies by group name
            - Optional[str]: Python version requirement (e.g., ">=3.10")
            - Dict: UV source configurations (indexes, sources)

    Example:
        ```python
        deps, optional, python_req, uv_sources = parse_pyproject_toml(Path("./"))

        print(deps)  # ["fastapi>=0.100", "uvicorn[standard]"]
        print(optional)  # {"dev": ["pytest", "black"]}
        print(python_req)  # ">=3.10"
        ```
    """
    pyproject_path = repo_root / "pyproject.toml"
    if not pyproject_path.exists():
        return [], {}, None, {}
    
    if tomllib is None:
        return [], {}, None, {}
    
    try:
        data = tomllib.loads(pyproject_path.read_text())

        project = data.get("project", {})
        deps = project.get("dependencies", [])
        optional = project.get("optional-dependencies", {})
        python_req = project.get("requires-python")

        # UV-specific sources (for PyTorch CUDA, etc.)
        uv_sources = {}
        tool_uv = data.get("tool", {}).get("uv", {})
        if "sources" in tool_uv:
            uv_sources["sources"] = tool_uv["sources"]
        if "index" in tool_uv:
            uv_sources["index"] = tool_uv["index"]

        return deps, optional, python_req, uv_sources
    except Exception as e:
        # HIGH FIX #19: Log parsing errors instead of silently returning empty results
        import warnings
        warnings.warn(
            f"Failed to parse pyproject.toml at {pyproject_path}: {e}. "
            "Dependencies from this file will be ignored.",
            stacklevel=2,
        )
        return [], {}, None, {}


def _parse_single_requirements_file(
    req_file: pl.Path,
    repo_root: pl.Path,
    seen_files: Optional[Set[pl.Path]] = None,
) -> List[str]:
    """
    Parse a single requirements file, handling -r includes recursively.

    Handles pip requirements file format including:
        - Package specifications (requests>=2.0)
        - Recursive includes (-r other.txt)
        - Constraint files (-c constraints.txt)
        - Comments and blank lines
        - Skips editable installs (-e) and URL-based packages

    Parameters:
        req_file (pl.Path):
            Path to the requirements file to parse.
        repo_root (pl.Path):
            Path to repository root for resolving relative paths.
        seen_files (Optional[Set[pl.Path]]):
            Set of already-parsed files to prevent infinite loops from circular includes.

    Returns:
        List[str]: List of dependency specification strings.
    """
    if seen_files is None:
        seen_files = set()
    
    # Prevent infinite loops from circular includes
    req_file = req_file.resolve()
    if req_file in seen_files:
        return []
    seen_files.add(req_file)
    
    deps = []
    try:
        for line in req_file.read_text().splitlines():
            line = line.strip()
            
            # Skip empty lines and comments
            if not line or line.startswith("#"):
                continue
            
            # Handle -r includes (recursive)
            if line.startswith("-r ") or line.startswith("--requirement "):
                parts = line.split(None, 1)
                if len(parts) > 1:
                    included = parts[1].strip()
                    included_path = req_file.parent / included
                    if included_path.exists():
                        deps.extend(_parse_single_requirements_file(
                            included_path, repo_root, seen_files
                        ))
                continue
            
            # Handle -c constraints (treat as deps for now)
            if line.startswith("-c ") or line.startswith("--constraint "):
                parts = line.split(None, 1)
                if len(parts) > 1:
                    constraint_file = parts[1].strip()
                    constraint_path = req_file.parent / constraint_file
                    if constraint_path.exists():
                        deps.extend(_parse_single_requirements_file(
                            constraint_path, repo_root, seen_files
                        ))
                continue
            
            # Skip other flags
            if line.startswith("-"):
                continue
            
            # Skip editable installs (local packages)
            if line.startswith("-e ") or line.startswith("--editable "):
                continue
            
            # Skip URLs for now (git+, http://, etc.)
            if "://" in line or line.startswith("git+"):
                continue
            
            deps.append(line)
    except Exception:
        pass
    
    return deps


def parse_requirements_txt(repo_root: pl.Path) -> List[str]:
    """
    Parse all requirements*.txt files in a repository.

    Finds and parses all files matching requirements*.txt pattern (e.g.,
    requirements.txt, requirements-dev.txt, requirements-test.txt).

    Parameters:
        repo_root (pl.Path):
            Path to the repository root.

    Returns:
        List[str]: Combined list of dependency specifications from all requirements files.

    Example:
        ```python
        deps = parse_requirements_txt(Path("./my_project"))
        print(deps)  # ["flask>=2.0", "sqlalchemy", "pytest"]
        ```
    """
    deps = []
    seen_files: set[pl.Path] = set()
    
    for req_file in repo_root.glob("requirements*.txt"):
        deps.extend(_parse_single_requirements_file(req_file, repo_root, seen_files))
    
    return deps


def parse_setup_py(repo_root: pl.Path) -> List[str]:
    """
    Parse setup.py for install_requires dependencies.

    Extracts dependencies from the install_requires argument in setup() calls
    using AST parsing. This is a best-effort parser that handles common patterns.

    Parameters:
        repo_root (pl.Path):
            Path to the repository root containing setup.py.

    Returns:
        List[str]: List of dependency specifications, empty if not found or on error.
    """
    setup_path = repo_root / "setup.py"
    if not setup_path.exists():
        return []
    
    try:
        content = setup_path.read_text()
        tree = ast.parse(content)
        
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                if isinstance(node.func, ast.Name) and node.func.id == "setup":
                    for keyword in node.keywords:
                        if keyword.arg == "install_requires":
                            if isinstance(keyword.value, ast.List):
                                return [
                                    ast.literal_eval(elt) 
                                    for elt in keyword.value.elts 
                                    if isinstance(elt, ast.Constant)
                                ]
    except Exception:
        pass
    return []


def parse_setup_cfg(repo_root: pl.Path) -> List[str]:
    """
    Parse setup.cfg for install_requires dependencies.

    Extracts dependencies from the [options] section's install_requires field.

    Parameters:
        repo_root (pl.Path):
            Path to the repository root containing setup.cfg.

    Returns:
        List[str]: List of dependency specifications.
    """
    setup_cfg = repo_root / "setup.cfg"
    if not setup_cfg.exists():
        return []
    
    deps = []
    config = configparser.ConfigParser()
    config.read(setup_cfg)
    
    if "options" in config and "install_requires" in config["options"]:
        raw = config["options"]["install_requires"]
        for line in raw.strip().splitlines():
            line = line.strip()
            if line and not line.startswith("#"):
                deps.append(line)
    return deps


def parse_pipfile(repo_root: pl.Path) -> List[str]:
    """
    Parse Pipfile for package dependencies.

    Extracts dependencies from the [packages] section of a Pipfile.
    Handles version specifications and wildcard (*) versions.

    Parameters:
        repo_root (pl.Path):
            Path to the repository root containing Pipfile.

    Returns:
        List[str]: List of dependency specifications.
    """
    pipfile = repo_root / "Pipfile"
    if not pipfile.exists():
        return []
    
    if tomllib is None:
        return []
    
    deps = []
    try:
        data = tomllib.loads(pipfile.read_text())
        packages = data.get("packages", {})
        
        for pkg_name, version_spec in packages.items():
            if isinstance(version_spec, str):
                if version_spec == "*":
                    deps.append(pkg_name)
                else:
                    deps.append(f"{pkg_name}{version_spec}")
            elif isinstance(version_spec, dict):
                # Handle complex specs like {version = ">=1.0", extras = ["security"]}
                version = version_spec.get("version", "*")
                if version == "*":
                    deps.append(pkg_name)
                else:
                    deps.append(f"{pkg_name}{version}")
    except Exception:
        pass
    return deps


def parse_pipfile_lock(repo_root: pl.Path) -> Dict[str, str]:
    """
    Parse Pipfile.lock for pinned package versions.

    Extracts exact versions from both 'default' and 'develop' sections.

    Parameters:
        repo_root (pl.Path):
            Path to the repository root containing Pipfile.lock.

    Returns:
        Dict[str, str]: Mapping of lowercase package name to version string.
    """
    lockfile = repo_root / "Pipfile.lock"
    if not lockfile.exists():
        return {}
    
    versions = {}
    try:
        data = json.loads(lockfile.read_text())
        for section in ["default", "develop"]:
            if section in data:
                for pkg, info in data[section].items():
                    if isinstance(info, dict) and "version" in info:
                        versions[pkg.lower()] = info["version"].lstrip("=")
    except Exception:
        pass
    return versions


def parse_poetry_lock(repo_root: pl.Path) -> Dict[str, str]:
    """
    Parse poetry.lock for pinned package versions.

    Extracts exact versions from the [[package]] entries in poetry.lock.

    Parameters:
        repo_root (pl.Path):
            Path to the repository root containing poetry.lock.

    Returns:
        Dict[str, str]: Mapping of lowercase package name to version string.
    """
    lockfile = repo_root / "poetry.lock"
    if not lockfile.exists():
        return {}
    
    if tomllib is None:
        return {}
    
    versions = {}
    try:
        data = tomllib.loads(lockfile.read_text())
        for pkg in data.get("package", []):
            name = pkg.get("name", "").lower()
            version = pkg.get("version", "")
            if name and version:
                versions[name] = version
    except Exception:
        pass
    return versions


def parse_uv_lockfile(lockfile: pl.Path) -> Dict[str, str]:
    """
    Parse uv.lock for pinned package versions.

    Extracts exact versions from the uv lockfile format (TOML-based).

    Parameters:
        lockfile (pl.Path):
            Path to the uv.lock file.

    Returns:
        Dict[str, str]: Mapping of package name to version string.
    """
    if not lockfile.exists():
        return {}
    
    if tomllib is None:
        return {}
    
    versions = {}
    try:
        data = tomllib.loads(lockfile.read_text())
        # uv.lock uses [[package]] array
        for pkg in data.get("package", []):
            name = pkg.get("name", "")
            version = pkg.get("version", "")
            if name and version:
                versions[name] = version
    except Exception:
        pass
    return versions


def parse_dep_spec(dep_string: str, source: str = "manifest") -> DependencySpec:
    """
    Parse a dependency string into a DependencySpec object.

    Handles standard pip dependency formats including version specifiers and extras.

    Parameters:
        dep_string (str):
            Dependency string to parse (e.g., "requests>=2.0", "uvicorn[standard]>=0.23").
        source (str):
            Source identifier for where this dependency was found. Default is "manifest".

    Returns:
        DependencySpec: Parsed dependency with name, version_spec, extras, and source.

    Example:
        ```python
        spec = parse_dep_spec("uvicorn[standard]>=0.23.0", source="pyproject")
        print(spec.name)  # "uvicorn"
        print(spec.extras)  # ["standard"]
        print(spec.version_spec)  # ">=0.23.0"
        ```
    """
    # Match: name[extras]version_spec
    pattern = r'^([a-zA-Z0-9_-]+)(?:\[([^\]]+)\])?(.*)$'
    match = re.match(pattern, dep_string.strip())
    
    if not match:
        return DependencySpec(name=dep_string.strip(), source=source)
    
    name = match.group(1)
    extras_str = match.group(2)
    version_spec = match.group(3).strip() if match.group(3) else None
    
    extras = [e.strip() for e in extras_str.split(",")] if extras_str else []
    
    return DependencySpec(
        name=name,
        version_spec=version_spec if version_spec else None,
        extras=extras,
        source=source,
    )
