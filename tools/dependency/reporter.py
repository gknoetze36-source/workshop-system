"""
===============================================================================
PHANTA Dependency Auditor v2.0
Console Reporter
===============================================================================

Formats dependency audit results into a human-readable report.

Responsibilities
----------------
• Format audit results
• Produce plain-text reports

This module performs NO analysis.
"""

from __future__ import annotations

from typing import List

from .models import AuditReport, Statistics


# =============================================================================
# Console Reporter
# =============================================================================

class ConsoleReporter:
    """
    Generates a human-readable text report.
    """

    def generate(
        self,
        report: AuditReport,
        statistics: Statistics,
    ) -> str:
        """
        Generate a formatted text report.
        """

        lines: List[str] = []

        # ---------------------------------------------------------------------
        # Header
        # ---------------------------------------------------------------------

        lines.append("=" * 72)
        lines.append("PHANTA Dependency Audit Report")
        lines.append("=" * 72)
        lines.append("")

        # ---------------------------------------------------------------------
        # Summary
        # ---------------------------------------------------------------------

        lines.append("SUMMARY")
        lines.append("-" * 72)
        lines.append(f"Modules:                  {statistics.modules}")
        lines.append(f"Dependencies:             {statistics.dependencies}")
        lines.append(
            f"Average Dependencies:     {statistics.average_dependencies:.2f}"
        )
        lines.append(
            f"Health Score:             {statistics.health_score}/100"
        )
        lines.append("")

        # ---------------------------------------------------------------------
        # Metrics
        # ---------------------------------------------------------------------

        lines.append("ANALYSIS")
        lines.append("-" * 72)
        lines.append(f"Syntax Errors:            {statistics.syntax_errors}")
        lines.append(f"Import Issues:            {statistics.import_issues}")
        lines.append(
            f"Duplicate Imports:        {statistics.duplicate_imports}"
        )
        lines.append(f"Circular Dependencies:   {statistics.cycles}")
        lines.append(
            f"Architecture Violations:  {statistics.architecture_violations}"
        )
        lines.append("")

        # ---------------------------------------------------------------------
        # Dependency Metrics
        # ---------------------------------------------------------------------

        lines.append("DEPENDENCY METRICS")
        lines.append("-" * 72)

        if statistics.most_imports_module:
            lines.append(
                f"Most Imports:             "
                f"{statistics.most_imports_module} "
                f"({statistics.most_imports_count})"
            )

        if statistics.most_depended_module:
            lines.append(
                f"Most Imported Module:     "
                f"{statistics.most_depended_module} "
                f"({statistics.most_depended_count})"
            )

        lines.append("")

        # ---------------------------------------------------------------------
        # Syntax Errors
        # ---------------------------------------------------------------------

        if report.syntax:

            lines.append("SYNTAX ERRORS")
            lines.append("-" * 72)

            for issue in report.syntax:

                lines.append(
                    f"{issue.module}:{issue.line}"
                )

                lines.append(
                    f"    {issue.message}"
                )

            lines.append("")

        # ---------------------------------------------------------------------
        # Import Issues
        # ---------------------------------------------------------------------

        if report.imports:

            lines.append("IMPORT ISSUES")
            lines.append("-" * 72)

            for issue in report.imports:

                lines.append(
                    f"{issue.module}:{issue.line}"
                )

                lines.append(
                    f"    {issue.issue}"
                )

            lines.append("")

        # ---------------------------------------------------------------------
        # Duplicate Imports
        # ---------------------------------------------------------------------

        if report.duplicates:

            lines.append("DUPLICATE IMPORTS")
            lines.append("-" * 72)

            for duplicate in report.duplicates:

                lines.append(
                    f"{duplicate.module}:{duplicate.line}"
                )

                lines.append(
                    f"    {duplicate.import_name}"
                )

            lines.append("")

        # ---------------------------------------------------------------------
        # Circular Dependencies
        # ---------------------------------------------------------------------

        if report.cycles:

            lines.append("CIRCULAR DEPENDENCIES")
            lines.append("-" * 72)

            for cycle in report.cycles:

                lines.append(
                    " -> ".join(cycle.modules)
                )

            lines.append("")

        # ---------------------------------------------------------------------
        # Architecture Violations
        # ---------------------------------------------------------------------

        if report.architecture:

            lines.append("ARCHITECTURE VIOLATIONS")
            lines.append("-" * 72)

            for violation in report.architecture:

                lines.append(
                    f"{violation.source_module}"
                )

                lines.append(
                    f"    imports {violation.target_module}"
                )

                lines.append(
                    f"    Rule: {violation.rule}"
                )

                lines.append(
                    f"    {violation.description}"
                )

                lines.append("")

        return "\n".join(lines)

    # -------------------------------------------------------------------------

    def print(
        self,
        report: AuditReport,
        statistics: Statistics,
    ) -> None:
        """
        Print the report to stdout.
        """

        print(self.generate(report, statistics))


# =============================================================================
# Convenience Functions
# =============================================================================

def generate_report(
    report: AuditReport,
    statistics: Statistics,
) -> str:
    """
    Generate a text report.
    """

    return ConsoleReporter().generate(
        report,
        statistics,
    )


def print_report(
    report: AuditReport,
    statistics: Statistics,
) -> None:
    """
    Print a text report.
    """

    ConsoleReporter().print(
        report,
        statistics,
    )