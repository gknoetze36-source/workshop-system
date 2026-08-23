"""
===============================================================================
PHANTA Dependency Auditor v2.0
Import Checker
===============================================================================

Validates parsed import statements.

Responsibilities
----------------
• Validate ImportRecord objects
• Detect malformed imports
• Detect self-imports
• Produce ImportIssue objects

This module performs NO dependency graph analysis.
"""

from __future__ import annotations

from typing import Iterable, List

from .models import ImportIssue, ImportRecord


# =============================================================================
# Import Checker
# =============================================================================

class ImportChecker:
    """
    Validates parsed imports.
    """

    # -------------------------------------------------------------------------

    def check_import(
        self,
        record: ImportRecord,
    ) -> List[ImportIssue]:
        """
        Validate a single import.
        """

        issues: List[ImportIssue] = []

        imported = record.imported_module.strip()

        # -------------------------------------------------------------
        # Empty import
        # -------------------------------------------------------------

        if not imported:

            issues.append(
                ImportIssue(
                    module=record.module,
                    line=record.line,
                    import_name=record.imported_module,
                    issue="Empty import target.",
                )
            )

            return issues

        # -------------------------------------------------------------
        # Self import
        # -------------------------------------------------------------

        if imported == record.module:

            issues.append(
                ImportIssue(
                    module=record.module,
                    line=record.line,
                    import_name=record.imported_module,
                    issue="Module imports itself.",
                )
            )

        # -------------------------------------------------------------
        # Invalid leading/trailing dot notation
        # -------------------------------------------------------------

        if imported.endswith("."):

            issues.append(
                ImportIssue(
                    module=record.module,
                    line=record.line,
                    import_name=record.imported_module,
                    issue="Import ends with '.'.",
                )
            )

        # -------------------------------------------------------------
        # Consecutive dots
        #
        # Ignore valid relative imports:
        #     .
        #     ..
        #     ...
        # -------------------------------------------------------------

        if "..." not in imported and ".." in imported and not imported.startswith("."):

            issues.append(
                ImportIssue(
                    module=record.module,
                    line=record.line,
                    import_name=record.imported_module,
                    issue="Suspicious consecutive dots in import.",
                )
            )

        return issues

    # -------------------------------------------------------------------------

    def check_imports(
        self,
        imports: Iterable[ImportRecord],
    ) -> List[ImportIssue]:
        """
        Validate an iterable of ImportRecord objects.
        """

        issues: List[ImportIssue] = []

        for record in imports:
            issues.extend(
                self.check_import(record)
            )

        return issues


# =============================================================================
# Convenience Functions
# =============================================================================

def check_import(
    record: ImportRecord,
) -> List[ImportIssue]:
    """
    Validate a single import.
    """

    return ImportChecker().check_import(record)


def check_imports(
    imports: Iterable[ImportRecord],
) -> List[ImportIssue]:
    """
    Validate multiple imports.
    """

    return ImportChecker().check_imports(imports)