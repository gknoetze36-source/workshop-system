"""
===============================================================================
PHANTA Dependency Auditor v2.0
JSON Report Exporter
===============================================================================

Exports dependency audit results as JSON.

Responsibilities
----------------
• Convert AuditReport into JSON
• Write JSON reports to disk

This module performs NO analysis.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .models import AuditReport, Statistics


# =============================================================================
# JSON Report Exporter
# =============================================================================

class JsonReportExporter:
    """
    Exports audit reports as JSON.
    """

    # -------------------------------------------------------------------------

    def to_dict(
        self,
        report: AuditReport,
        statistics: Statistics,
    ) -> dict[str, Any]:
        """
        Convert an audit report into a JSON-serializable dictionary.
        """

        return {

            "statistics": {

                "modules": statistics.modules,
                "dependencies": statistics.dependencies,
                "average_dependencies": statistics.average_dependencies,

                "most_imports_module": statistics.most_imports_module,
                "most_imports_count": statistics.most_imports_count,

                "most_depended_module": statistics.most_depended_module,
                "most_depended_count": statistics.most_depended_count,

                "syntax_errors": statistics.syntax_errors,
                "import_issues": statistics.import_issues,
                "duplicate_imports": statistics.duplicate_imports,
                "cycles": statistics.cycles,
                "architecture_violations": statistics.architecture_violations,

                "health_score": statistics.health_score,
            },

            "summary": {

                "modules": report.summary.modules,
                "imports": report.summary.imports,
                "cycles": report.summary.cycles,

                "syntax_errors": report.summary.syntax_errors,
                "import_errors": report.summary.import_errors,
                "duplicate_imports": report.summary.duplicate_imports,
                "architecture_violations":
                    report.summary.architecture_violations,

                "health_score": report.summary.health_score,
            },

            "syntax": [
                {
                    "module": issue.module,
                    "path": str(issue.path),
                    "line": issue.line,
                    "message": issue.message,
                }
                for issue in report.syntax
            ],

            "imports": [
                {
                    "module": issue.module,
                    "line": issue.line,
                    "import_name": issue.import_name,
                    "issue": issue.issue,
                }
                for issue in report.imports
            ],

            "duplicates": [
                {
                    "module": duplicate.module,
                    "line": duplicate.line,
                    "import_name": duplicate.import_name,
                    "import_type": duplicate.import_type,
                }
                for duplicate in report.duplicates
            ],

            "cycles": [
                {
                    "modules": cycle.modules,
                    "length": cycle.length,
                }
                for cycle in report.cycles
            ],

            "architecture": [
                {
                    "source_module": violation.source_module,
                    "target_module": violation.target_module,
                    "rule": violation.rule,
                    "description": violation.description,
                }
                for violation in report.architecture
            ],
        }

    # -------------------------------------------------------------------------

    def to_json(
        self,
        report: AuditReport,
        statistics: Statistics,
        *,
        indent: int = 4,
    ) -> str:
        """
        Return a formatted JSON string.
        """

        return json.dumps(
            self.to_dict(report, statistics),
            indent=indent,
            sort_keys=False,
        )

    # -------------------------------------------------------------------------

    def write(
        self,
        filename: str | Path,
        report: AuditReport,
        statistics: Statistics,
        *,
        indent: int = 4,
    ) -> Path:
        """
        Write the JSON report to disk.
        """

        path = Path(filename)

        path.write_text(
            self.to_json(
                report,
                statistics,
                indent=indent,
            ),
            encoding="utf-8",
        )

        return path


# =============================================================================
# Convenience Functions
# =============================================================================

def export_json(
    filename: str | Path,
    report: AuditReport,
    statistics: Statistics,
) -> Path:
    """
    Export a JSON report.
    """

    return JsonReportExporter().write(
        filename,
        report,
        statistics,
    )


def report_as_json(
    report: AuditReport,
    statistics: Statistics,
    *,
    indent: int = 4,
) -> str:
    """
    Return the report as a JSON string.
    """

    return JsonReportExporter().to_json(
        report,
        statistics,
        indent=indent,
    )