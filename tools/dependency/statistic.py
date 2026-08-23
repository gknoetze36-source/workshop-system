"""
===============================================================================
PHANTA Dependency Auditor v2.0
Statistics Engine
===============================================================================

Calculates project dependency statistics.

Responsibilities
----------------
• Calculate dependency metrics
• Calculate project health score
• Produce Statistics objects

This module performs NO dependency analysis.
"""

from __future__ import annotations

from typing import Iterable

from .graph import DependencyGraph
from .models import (
    ArchitectureViolation,
    Cycle,
    DuplicateImport,
    ImportIssue,
    Statistics,
    SyntaxIssue,
)


# =============================================================================
# Statistics Calculator
# =============================================================================

class StatisticsCalculator:
    """
    Calculates project statistics from analysis results.
    """

    def calculate(
        self,
        graph: DependencyGraph,
        *,
        syntax_errors: Iterable[SyntaxIssue] = (),
        import_issues: Iterable[ImportIssue] = (),
        duplicate_imports: Iterable[DuplicateImport] = (),
        cycles: Iterable[Cycle] = (),
        architecture_violations: Iterable[ArchitectureViolation] = (),
    ) -> Statistics:
        """
        Calculate project statistics.
        """

        syntax_errors = list(syntax_errors)
        import_issues = list(import_issues)
        duplicate_imports = list(duplicate_imports)
        cycles = list(cycles)
        architecture_violations = list(architecture_violations)

        stats = Statistics()

        # -------------------------------------------------------------
        # Basic graph metrics
        # -------------------------------------------------------------

        stats.modules = graph.module_count
        stats.dependencies = graph.edge_count

        if stats.modules:
            stats.average_dependencies = (
                stats.dependencies / stats.modules
            )

        # -------------------------------------------------------------
        # Module with most outgoing dependencies
        # -------------------------------------------------------------

        most_imports = 0
        most_imports_module = ""

        for node in graph.nodes():

            if node.import_count > most_imports:

                most_imports = node.import_count
                most_imports_module = node.name

        stats.most_imports_module = most_imports_module
        stats.most_imports_count = most_imports

        # -------------------------------------------------------------
        # Module with most incoming dependencies
        # -------------------------------------------------------------

        most_depended = 0
        most_depended_module = ""

        for node in graph.nodes():

            if node.incoming_count > most_depended:

                most_depended = node.incoming_count
                most_depended_module = node.name

        stats.most_depended_module = most_depended_module
        stats.most_depended_count = most_depended

        # -------------------------------------------------------------
        # Analysis results
        # -------------------------------------------------------------

        stats.syntax_errors = len(syntax_errors)
        stats.import_issues = len(import_issues)
        stats.duplicate_imports = len(duplicate_imports)
        stats.cycles = len(cycles)
        stats.architecture_violations = len(
            architecture_violations
        )

        # -------------------------------------------------------------
        # Health Score
        # -------------------------------------------------------------

        score = 100

        score -= stats.syntax_errors * 10
        score -= stats.import_issues * 2
        score -= stats.duplicate_imports
        score -= stats.cycles * 5
        score -= stats.architecture_violations * 5

        stats.health_score = max(0, score)

        return stats


# =============================================================================
# Convenience Function
# =============================================================================

def calculate_statistics(
    graph: DependencyGraph,
    **kwargs,
) -> Statistics:
    """
    Calculate project statistics.
    """

    return StatisticsCalculator().calculate(
        graph,
        **kwargs,
    )