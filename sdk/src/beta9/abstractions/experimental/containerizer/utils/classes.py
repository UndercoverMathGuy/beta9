"""
Data classes for dependency discovery.

This module contains all dataclasses used for representing dependency information
discovered from Python projects.
"""
import pathlib as pl
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set


@dataclass
class DependencySpec:
    """
    Specification for a single Python package dependency.

    Represents a Python package with its name, version constraints, extras,
    and metadata about how it was discovered.

    Attributes:
        name (str):
            The PyPI package name (e.g., "requests", "numpy").
        version_spec (Optional[str]):
            Version specification string (e.g., ">=2.0", "==1.5.0"). None if unspecified.
        extras (list[str]):
            List of extras to install (e.g., ["standard"] for uvicorn[standard]).
        source (str):
            How this dependency was discovered. One of:
            - "lockfile": From uv.lock, poetry.lock, or Pipfile.lock
            - "pyproject": From pyproject.toml
            - "requirements": From requirements.txt
            - "setup.py", "setup.cfg", "pipfile": From respective files
            - "ast:importlib.metadata": From code scanning with version from metadata
            - "ast:static_map": From code scanning with static mapping
            - "ast:assumed": From code scanning, package name assumed
        is_conditional (bool):
            True if the import was found in a try/except block.

    Example:
        ```python
        spec = DependencySpec(
            name="uvicorn",
            version_spec=">=0.23.0",
            extras=["standard"],
            source="pyproject",
        )
        print(spec.to_pip_spec())  # "uvicorn[standard]>=0.23.0"
        ```
    """

    name: str
    version_spec: Optional[str] = None
    extras: List[str] = field(default_factory=list)
    source: str = "ast"
    is_conditional: bool = False

    def to_pip_spec(self) -> str:
        """
        Convert to a pip-installable specification string.

        Returns:
            str: A string suitable for pip install (e.g., "requests>=2.0",
                "uvicorn[standard]>=0.23.0").
        """
        base = self.name
        if self.extras:
            base = f"{self.name}[{','.join(self.extras)}]"
        if self.version_spec:
            return f"{base}{self.version_spec}"
        return base

    def __hash__(self):
        return hash(self.name.lower())

    def __eq__(self, other):
        if isinstance(other, DependencySpec):
            return self.name.lower() == other.name.lower()
        return False


@dataclass
class UnresolvedPackage:
    """
    A package import that could not be automatically resolved to a PyPI package.

    When an import is found in the source code but cannot be confidently mapped
    to a PyPI package name, it is recorded as unresolved.

    Attributes:
        import_name (str):
            The import name found in the source code (e.g., "mymodule").
        guessed_package (str):
            Best guess at the PyPI package name.
        reason (str):
            Explanation of why the package couldn't be resolved.
        suggestions (list[str]):
            List of suggested actions to resolve the issue.
    """

    import_name: str
    guessed_package: str
    reason: str
    suggestions: List[str] = field(default_factory=list)


@dataclass
class ProjectDependencies:
    """
    Complete dependency information for a Python project.

    This is the main result object returned by discover_dependencies(). It contains
    all discovered dependencies categorized by type, along with metadata about the
    discovery process.

    Attributes:
        python_version (Optional[str]):
            Python version requirement from pyproject.toml (e.g., ">=3.10").
        third_party (Dict[str, DependencySpec]):
            Dictionary mapping lowercase package names to their DependencySpec.
        stdlib (Set[str]):
            Set of standard library modules imported by the project.
        local (Set[str]):
            Set of local package/module names (excluded from third_party).
        uv_sources (dict):
            UV-specific source configurations (e.g., PyTorch CUDA indexes).
        has_lockfile (bool):
            True if dependencies were read from a lockfile (uv.lock, poetry.lock, etc.).
        unresolved (List[UnresolvedPackage]):
            List of imports that couldn't be resolved to PyPI packages.

    Example:
        ```python
        deps = discover_dependencies("./my_project")

        # Get all pip specs
        for spec in deps.to_pip_specs():
            print(spec)  # "requests>=2.28.0"

        # Check specific package
        if "torch" in deps.third_party:
            torch_spec = deps.third_party["torch"]
            print(f"PyTorch version: {torch_spec.version_spec}")

        # Serialize to JSON
        import json
        print(json.dumps(deps.to_dict(), indent=2))
        ```
    """

    python_version: Optional[str] = None
    third_party: Dict[str, DependencySpec] = field(default_factory=dict)
    stdlib: Set[str] = field(default_factory=set)
    local: Set[str] = field(default_factory=set)
    uv_sources: dict = field(default_factory=dict)
    has_lockfile: bool = False
    unresolved: List[UnresolvedPackage] = field(default_factory=list)

    def to_pip_specs(self) -> List[str]:
        """
        Get a list of pip-installable package specifications.

        Returns:
            List[str]: List of pip specs (e.g., ["requests>=2.0", "flask==2.3.0"]).
        """
        return [spec.to_pip_spec() for spec in self.third_party.values()]

    def to_dict(self) -> dict:
        """
        Convert to a dictionary for JSON serialization.

        Returns:
            dict: Dictionary representation of all dependency information.
        """
        return {
            "python_version": self.python_version,
            "packages": {
                name: {
                    "pip_spec": spec.to_pip_spec(),
                    "version": spec.version_spec,
                    "extras": spec.extras,
                    "source": spec.source,
                    "conditional": spec.is_conditional,
                }
                for name, spec in self.third_party.items()
            },
            "has_lockfile": self.has_lockfile,
            "unresolved": [
                {
                    "import_name": u.import_name,
                    "guessed_package": u.guessed_package,
                    "reason": u.reason,
                    "suggestions": u.suggestions,
                }
                for u in self.unresolved
            ],
        }


@dataclass
class MonorepoPackage:
    """
    A package within a monorepo workspace.

    Represents a sub-package discovered in a monorepo structure, typically found
    in directories like packages/, libs/, or defined in workspace configurations.

    Attributes:
        name (str):
            The package name (usually the directory name).
        path (pl.Path):
            Absolute path to the package directory.
        has_pyproject (bool):
            True if the package has a pyproject.toml file.
        has_setup_py (bool):
            True if the package has a setup.py file.
        has_requirements (bool):
            True if the package has requirements*.txt files.
    """

    name: str
    path: pl.Path
    has_pyproject: bool = False
    has_setup_py: bool = False
    has_requirements: bool = False