"""
File system utilities for dependency discovery.

This module provides utilities for traversing Python project directories,
discovering local modules, and building directory trees while respecting
common exclusion patterns.

Constants:
    DEFAULT_EXCLUDE_DIRS: Set of directory names to skip during traversal
        (e.g., .git, __pycache__, node_modules, venv).
    MAX_READ_BYTES: Maximum bytes to read from a single file (2MB).
"""
import functools
import json
import os
import pathlib as pl
import re
from typing import Dict, List, Set

# Directories to skip by default during file traversal
DEFAULT_EXCLUDE_DIRS: Set[str] = {
    ".git",
    ".hg",
    ".svn",
    "__pycache__",
    ".ipynb_checkpoints",
    "node_modules",
    ".venv",
    "venv",
    ".mypy_cache",
    ".ruff_cache",
}

# Max bytes we'll read from a file (protects against huge artifacts)
MAX_READ_BYTES: int = 2_000_000  # ~2MB


def _should_skip_dir(dirname: str) -> bool:
    """
    Check if a directory should be skipped during traversal.

    Parameters:
        dirname (str): The directory name to check.

    Returns:
        bool: True if the directory should be skipped.
    """
    if dirname in DEFAULT_EXCLUDE_DIRS:
        return True
    if dirname.startswith("."):
        return True
    return False


def build_tree(path: str) -> List:
    """
    Build a directory tree while respecting common exclusion patterns.

    Parameters:
        path (str): The path to the directory to build the tree for.

    Returns:
        List: A list of directory entries, where each entry is either a file name or a dictionary
            containing a directory name and its contents.
    """
    items = []
    path = pl.Path(path)
    for entry in sorted(path.iterdir(), key=lambda e: (not e.is_dir(), e.name.lower())):
        if entry.is_dir():
            if _should_skip_dir(entry.name):
                continue
            if entry.is_symlink():
                continue
            items.append({entry.name: build_tree(entry)})
        elif entry.is_file():
            items.append(entry.name)

    return items

@functools.lru_cache(maxsize=64)
def _discover_local_toplevel_modules(repo_root: str | pl.Path) -> Set[str]:
    """
    Discover local top-level modules and packages in a repository.

    Identifies local Python modules that should be excluded from third-party
    dependency detection. This prevents local imports from being incorrectly
    identified as external packages.

    Detects:
        1. Top-level .py files (e.g., main.py -> "main")
        2. Traditional packages with __init__.py
        3. Namespace packages / directories containing .py files (PEP 420)
        4. Rust/native extension modules defined in pyproject.toml (maturin)

    Parameters:
        repo_root (str | pl.Path):
            Path to the repository root.

    Returns:
        Set[str]: Set of local module/package names.

    Example:
        ```python
        local_modules = _discover_local_toplevel_modules("./my_project")
        # {"myapp", "utils", "main", "config"}
        ```
    """
    root = pl.Path(repo_root)
    out: set[str] = set()
    
    # Check for native extension modules in pyproject.toml (e.g., rustbpe)
    pyproject = root / "pyproject.toml"
    if pyproject.exists():
        try:
            content = pyproject.read_text()
            # Look for maturin module-name or setuptools ext_modules
            # Maturin: module-name = "rustbpe"
            match = re.search(r'module-name\s*=\s*"([^"]+)"', content)
            if match:
                out.add(match.group(1))
            # Also check for [tool.maturin] python-source
            if "[tool.maturin]" in content:
                # The directory containing Cargo.toml is likely a native module
                for cargo in root.rglob("Cargo.toml"):
                    if cargo.parent.name not in DEFAULT_EXCLUDE_DIRS:
                        out.add(cargo.parent.name)
        except Exception:
            pass
    
    for base in (root, root / "src"):
        if not base.exists():
            continue
        # top-level .py modules
        for p in base.glob("*.py"):
            if p.name != "__init__.py":
                out.add(p.stem)
        # top-level directories
        for p in base.iterdir():
            if not p.is_dir() or p.is_symlink():
                continue
            if p.name.startswith("."):
                continue
            if p.name in DEFAULT_EXCLUDE_DIRS:
                continue
            
            # Traditional package with __init__.py
            if (p / "__init__.py").exists():
                out.add(p.name)
                continue
            
            # Namespace package / directory with Python files (PEP 420)
            # Check if directory contains any .py files
            has_py_files = any(p.glob("*.py")) or any(p.glob("**/*.py"))
            if has_py_files:
                out.add(p.name)
    
    return out
