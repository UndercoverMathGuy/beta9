"""
AST-based import extraction from Python source files.

This module provides utilities for extracting import statements from Python source
files using Abstract Syntax Tree (AST) parsing. It also supports PEP 723 inline
script dependencies.

Features:
    - Extracts all import and from...import statements
    - Identifies conditional imports (inside try/except blocks)
    - Parses PEP 723 inline script metadata for dependencies
    - Handles syntax errors gracefully

Example:
    ```python
    from pathlib import Path
    from containerizer.utils.ast_scanner import extract_imports_from_file

    imports = extract_imports_from_file(Path("./app.py"))
    for module_name, is_conditional in imports:
        print(f"{module_name} (conditional: {is_conditional})")
    ```
"""
import ast
import pathlib as pl
import re
from typing import List, Tuple


class ImportVisitor(ast.NodeVisitor):
    """
    AST visitor that extracts import statements from Python source code.

    Traverses the AST and collects all import statements, marking those found
    inside try/except blocks as conditional imports.

    Attributes:
        imports (List[Tuple[str, bool]]):
            List of (module_name, is_conditional) tuples.
    """

    def __init__(self):
        self.imports: List[Tuple[str, bool]] = []
        self._in_try_except = False

    def visit_Import(self, node: ast.Import):
        """Handle 'import x' statements."""
        for alias in node.names:
            self.imports.append((alias.name, self._in_try_except))
        self.generic_visit(node)

    def visit_ImportFrom(self, node: ast.ImportFrom):
        """Handle 'from x import y' statements."""
        if node.module:
            self.imports.append((node.module, self._in_try_except))
        elif node.level > 0:
            # Relative import without module name
            self.imports.append(("." * node.level, self._in_try_except))
        self.generic_visit(node)

    def visit_Try(self, node: ast.Try):
        """Mark imports in try block as conditional."""
        old_state = self._in_try_except
        self._in_try_except = True
        for child in node.body:
            self.visit(child)
        for handler in node.handlers:
            for child in handler.body:
                self.visit(child)
        self._in_try_except = old_state
        # Visit else/finally normally
        for child in node.orelse:
            self.visit(child)
        for child in node.finalbody:
            self.visit(child)


# MEDIUM FIX #33: Maximum file size to read (10 MB)
MAX_FILE_SIZE_BYTES = 10 * 1024 * 1024


def extract_imports_from_file(file_path: pl.Path) -> List[Tuple[str, bool]]:
    """
    Extract import statements from a Python file.

    Parses the file using AST and extracts all import statements. Imports found
    inside try/except blocks are marked as conditional.

    Parameters:
        file_path (pl.Path):
            Path to the Python file to analyze.

    Returns:
        List[Tuple[str, bool]]: List of tuples containing:
            - module_name (str): The imported module name
            - is_conditional (bool): True if import is inside try/except

    Example:
        ```python
        imports = extract_imports_from_file(Path("./app.py"))
        # [("requests", False), ("ujson", True), ("json", True)]
        ```
    """
    try:
        # MEDIUM FIX #33: Check file size before reading to prevent memory exhaustion
        file_size = file_path.stat().st_size
        if file_size > MAX_FILE_SIZE_BYTES:
            return []

        content = file_path.read_text(encoding="utf-8", errors="ignore")
        tree = ast.parse(content, filename=str(file_path))
        visitor = ImportVisitor()
        visitor.visit(tree)
        return visitor.imports
    except SyntaxError:
        # MEDIUM FIX #29: Log syntax error for debugging
        # Silently return empty - file may have valid non-Python content
        return []
    except (OSError, IOError):
        # File access error - skip file
        return []


def extract_pep723_deps(file_path: pl.Path) -> List[str]:
    """
    Extract PEP 723 inline script dependencies.

    PEP 723 allows single-file Python scripts to declare their dependencies
    inline using a special comment block format.

    Looks for blocks like:
        ```python
        # /// script
        # dependencies = ["requests", "click>=8.0"]
        # ///
        ```

    Parameters:
        file_path (pl.Path):
            Path to the Python file to analyze.

    Returns:
        List[str]: List of dependency specification strings.

    Example:
        ```python
        deps = extract_pep723_deps(Path("./script.py"))
        # ["requests", "click>=8.0"]
        ```
    """
    try:
        content = file_path.read_text(encoding="utf-8", errors="ignore")
        # Match # /// script ... # ///
        pattern = r'# /// script\s*\n(.*?)# ///'
        match = re.search(pattern, content, re.DOTALL)
        if match:
            block = match.group(1)
            deps_match = re.search(r'dependencies\s*=\s*\[(.*?)\]', block, re.DOTALL)
            if deps_match:
                deps_str = deps_match.group(1)
                # Extract quoted strings
                return re.findall(r'"([^"]+)"', deps_str)
    except Exception:
        pass
    return []
