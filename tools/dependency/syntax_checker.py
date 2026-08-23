"""
===============================================================================
PHANTA Dependency Auditor v2.0
Syntax Checker
===============================================================================

Validates Python source files for syntax errors.

Responsibilities
----------------
• Parse Python source files
• Detect syntax errors
• Produce SyntaxIssue objects

This module performs NO dependency analysis.
"""

from __future__ import annotations

import ast
from pathlib import Path
from typing import List

from .models import SourceFile, SyntaxIssue


# =============================================================================
# Syntax Checker
# =============================================================================

class SyntaxChecker:
    """
    Checks Python source files for syntax errors.
    """

    # -------------------------------------------------------------------------

    def check_file(
        self,
        source_file: SourceFile,
    ) -> List[SyntaxIssue]:
        """
        Check a single Python file.

        Returns an empty list if the file is valid.
        """

        try:

            source = source_file.path.read_text(
                encoding="utf-8",
            )

            ast.parse(
                source,
                filename=str(source_file.path),
            )

            return []

        except SyntaxError as exc:

            return [
                SyntaxIssue(
                    module=source_file.module,
                    path=source_file.path,
                    line=exc.lineno or 0,
                    message=exc.msg,
                )
            ]

        except Exception as exc:

            return [
                SyntaxIssue(
                    module=source_file.module,
                    path=source_file.path,
                    line=0,
                    message=str(exc),
                )
            ]

    # -------------------------------------------------------------------------

    def check_project(
        self,
        files: List[SourceFile],
    ) -> List[SyntaxIssue]:
        """
        Check an entire project for syntax errors.
        """

        issues: List[SyntaxIssue] = []

        for source_file in files:
            issues.extend(
                self.check_file(source_file)
            )

        return issues


# =============================================================================
# Convenience Functions
# =============================================================================

def check_file(
    source_file: SourceFile,
) -> List[SyntaxIssue]:
    """
    Check a single Python file.
    """

    return SyntaxChecker().check_file(source_file)


def check_project(
    files: List[SourceFile],
) -> List[SyntaxIssue]:
    """
    Check an entire project.
    """

    return SyntaxChecker().check_project(files)