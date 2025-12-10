"""
Containerizer - Automatic Python dependency discovery.

This module provides utilities for automatically detecting Python package
dependencies from project source code, lockfiles, and manifests.
"""
from .dependencies import discover_dependencies, get_pip_packages
from .utils.classes import DependencySpec, ProjectDependencies, UnresolvedPackage

__all__ = [
    "discover_dependencies",
    "get_pip_packages",
    "DependencySpec",
    "ProjectDependencies",
    "UnresolvedPackage",
]
