"""
===============================================================================
PHANTA Dependency Auditor v2.0
Python AST Parser
===============================================================================

Parses Python source files into structured ImportRecord objects.

Responsibilities
----------------
• Parse Python source code using the AST
• Extract import statements
• Produce ImportRecord objects

This module performs NO dependency analysis.
"""

from __future__ import annotations

import ast
from pathlib import Path
from typing import List

from .models import ImportRecord


# =============================================================================
# Dependency Parser
# =============================================================================

class DependencyParser:
    """
    Parses Python source files and extracts import information.
    """

    # -------------------------------------------------------------------------

    def parse_file(
        self,
        module_name: str,
        file_path: str | Path,
    ) -> List[ImportRecord]:
        """
        Parse one Python file.

        Parameters
        ----------
        module_name:
            Python module name.

        file_path:
            Path to the source file.
        """

        path = Path(file_path)

        source = path.read_text(
            encoding="utf-8",
        )

        return self.parse_source(
            module_name,
            source,
        )

    # -------------------------------------------------------------------------

    def parse_source(
        self,
        module_name: str,
        source: str,
    ) -> List[ImportRecord]:
        """
        Parse Python source code.
        """

        tree = ast.parse(source)

        imports: List[ImportRecord] = []

        for node in ast.walk(tree):

            # -------------------------------------------------------------
            # import package
            #
            # import os
            # import pathlib
            # import services.booking
            # -------------------------------------------------------------

            if isinstance(node, ast.Import):

                for alias in node.names:

                    imports.append(
                        ImportRecord(
                            module=module_name,
                            imported_module=alias.name,
                            line=node.lineno,
                            import_type="import",
                        )
                    )

            # -------------------------------------------------------------
            # from package import x
            #
            # from pathlib import Path
            # from services import booking
            # -------------------------------------------------------------

            elif isinstance(node, ast.ImportFrom):

                module = node.module or ""

                # Relative import (from . import foo)
                if node.level:

                    dots = "." * node.level

                    if module:
                        module = dots + module
                    else:
                        module = dots

                imports.append(
                    ImportRecord(
                        module=module_name,
                        imported_module=module,
                        line=node.lineno,
                        import_type="from",
                    )
                )

        return imports

    # -------------------------------------------------------------------------

    def parse_project(
        self,
        files,
    ) -> List[ImportRecord]:
        """
        Parse every discovered source file.
        """

        imports: List[ImportRecord] = []

        for source in files:

            try:

                imports.extend(
                    self.parse_file(
                        source.module,
                        source.path,
                    )
                )

            except SyntaxError:
                #
                # Syntax checking is handled elsewhere.
                #
                continue

        return imports


# =============================================================================
# Convenience Functions
# =============================================================================

def parse_python_file(
    module_name: str,
    file_path: str | Path,
) -> List[ImportRecord]:
    """
    Parse one Python file.
    """

    return DependencyParser().parse_file(
        module_name,
        file_path,
    )


def parse_project(
    files,
) -> List[ImportRecord]:
    """
    Parse an entire project.
    """

    return DependencyParser().parse_project(
        files,
    )