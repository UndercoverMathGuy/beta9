"""
Monorepo detection and handling.

This module provides utilities for detecting and handling monorepo structures
in Python projects. Monorepos contain multiple packages in a single repository,
often organized in directories like packages/, libs/, or defined via workspace
configurations.

Supported Monorepo Patterns:
    - UV workspaces ([tool.uv.workspace] in pyproject.toml)
    - Poetry workspaces ([tool.poetry.packages] in pyproject.toml)
    - Convention-based (packages/, libs/, modules/, components/, services/)

Example:
    ```python
    from pathlib import Path
    from containerizer.utils.monorepo import detect_monorepo

    packages = detect_monorepo(Path("./my_monorepo"))
    for pkg in packages:
        print(f"{pkg.name} at {pkg.path}")
    ```
"""
import pathlib as pl
import sys
from typing import List

# tomllib is Python 3.11+, fallback to tomli for older versions
if sys.version_info >= (3, 11):
    import tomllib
else:
    try:
        import tomli as tomllib
    except ImportError:
        tomllib = None  # type: ignore

from .classes import MonorepoPackage


def detect_monorepo(repo_root: pl.Path) -> List[MonorepoPackage]:
    """
    Detect if a repository is a monorepo and find its sub-packages.

    Searches for multiple packages within a repository by checking:
        1. UV workspace definitions in pyproject.toml
        2. Poetry workspace definitions in pyproject.toml
        3. Common monorepo directory conventions (packages/, libs/, etc.)

    Parameters:
        repo_root (pl.Path):
            Path to the repository root.

    Returns:
        List[MonorepoPackage]: List of detected sub-packages. Empty list if not
            a monorepo or no sub-packages found.

    Example:
        ```python
        packages = detect_monorepo(Path("./"))

        for pkg in packages:
            print(f"Found: {pkg.name}")
            if pkg.has_pyproject:
                print("  - Has pyproject.toml")
        ```
    """
    packages = []
    
    # Check for UV/Poetry workspace
    pyproject = repo_root / "pyproject.toml"
    if pyproject.exists() and tomllib is not None:
        try:
            data = tomllib.loads(pyproject.read_text())
            # UV workspace
            workspace_members = data.get("tool", {}).get("uv", {}).get("workspace", {}).get("members", [])
            # Poetry workspace
            if not workspace_members:
                workspace_members = data.get("tool", {}).get("poetry", {}).get("packages", [])
            
            for pattern in workspace_members:
                # Handle glob patterns like "packages/*"
                if "*" in pattern:
                    base_dir = repo_root / pattern.replace("/*", "").replace("*", "")
                    if base_dir.exists():
                        for subdir in base_dir.iterdir():
                            if subdir.is_dir() and (subdir / "pyproject.toml").exists():
                                packages.append(MonorepoPackage(
                                    name=subdir.name,
                                    path=subdir,
                                    has_pyproject=True,
                                ))
                else:
                    pkg_path = repo_root / pattern
                    if pkg_path.exists() and pkg_path.is_dir():
                        packages.append(MonorepoPackage(
                            name=pkg_path.name,
                            path=pkg_path,
                            has_pyproject=(pkg_path / "pyproject.toml").exists(),
                            has_setup_py=(pkg_path / "setup.py").exists(),
                        ))
        except Exception:
            pass
    
    # Check common monorepo directories
    for mono_dir in ["packages", "libs", "modules", "components", "services"]:
        mono_path = repo_root / mono_dir
        if mono_path.exists() and mono_path.is_dir():
            for subdir in mono_path.iterdir():
                if not subdir.is_dir():
                    continue
                if subdir.name.startswith("."):
                    continue
                
                has_pyproject = (subdir / "pyproject.toml").exists()
                has_setup = (subdir / "setup.py").exists()
                has_req = any(subdir.glob("requirements*.txt"))
                
                if has_pyproject or has_setup or has_req:
                    # Avoid duplicates
                    if not any(p.path == subdir for p in packages):
                        packages.append(MonorepoPackage(
                            name=subdir.name,
                            path=subdir,
                            has_pyproject=has_pyproject,
                            has_setup_py=has_setup,
                            has_requirements=has_req,
                        ))
    
    return packages
