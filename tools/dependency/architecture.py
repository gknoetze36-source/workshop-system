"""
===============================================================================
PHANTA Dependency Auditor v2.0
Architecture Validator
===============================================================================

Validates project architecture using configurable dependency rules.

Responsibilities
----------------
• Enforce dependency rules
• Detect architectural violations
• Produce ArchitectureViolation objects

This module performs NO graph construction.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List

from .graph import DependencyGraph
from .models import ArchitectureViolation


# =============================================================================
# Architecture Rule
# =============================================================================

@dataclass(slots=True)
class ArchitectureRule:
    """
    Defines a single architectural dependency rule.

    Example
    -------
    source_prefix = "ui"
    forbidden_prefix = "database"

    Prevents:
        ui.* -> database.*
    """

    source_prefix: str
    forbidden_prefix: str
    description: str


# =============================================================================
# Architecture Validator
# =============================================================================

class ArchitectureValidator:
    """
    Validates a DependencyGraph against architecture rules.
    """

    def __init__(
        self,
        rules: Iterable[ArchitectureRule] | None = None,
    ) -> None:

        self.rules = list(rules or [])

    # -------------------------------------------------------------------------

    def add_rule(
        self,
        rule: ArchitectureRule,
    ) -> None:
        """
        Register a new rule.
        """

        self.rules.append(rule)

    # -------------------------------------------------------------------------

    def validate(
        self,
        graph: DependencyGraph,
    ) -> List[ArchitectureViolation]:
        """
        Validate a dependency graph.
        """

        violations: List[ArchitectureViolation] = []

        for source, target in graph.edges():

            for rule in self.rules:

                if (
                    source.startswith(rule.source_prefix)
                    and target.startswith(rule.forbidden_prefix)
                ):

                    violations.append(
                        ArchitectureViolation(
                            source_module=source,
                            target_module=target,
                            rule=(
                                f"{rule.source_prefix} -> "
                                f"{rule.forbidden_prefix}"
                            ),
                            description=rule.description,
                        )
                    )

        return violations

    # -------------------------------------------------------------------------

    def has_violations(
        self,
        graph: DependencyGraph,
    ) -> bool:
        """
        Returns True if architecture violations exist.
        """

        return bool(self.validate(graph))


# =============================================================================
# Default Rules
# =============================================================================

def default_rules() -> List[ArchitectureRule]:
    """
    Returns a default set of architecture rules.
    """

    return [

        ArchitectureRule(
            source_prefix="ui",
            forbidden_prefix="database",
            description="UI layer must not access the database directly.",
        ),

        ArchitectureRule(
            source_prefix="routes",
            forbidden_prefix="database",
            description="Routes should use services instead of the database.",
        ),

        ArchitectureRule(
            source_prefix="api",
            forbidden_prefix="database",
            description="API layer must use services instead of direct database access.",
        ),

        ArchitectureRule(
            source_prefix="tests",
            forbidden_prefix="production",
            description="Tests should not depend on production entry points.",
        ),
    ]


# =============================================================================
# Convenience Functions
# =============================================================================

def validate_architecture(
    graph: DependencyGraph,
    rules: Iterable[ArchitectureRule] | None = None,
) -> List[ArchitectureViolation]:
    """
    Validate a dependency graph.
    """

    validator = ArchitectureValidator(
        rules or default_rules()
    )

    return validator.validate(graph)


def has_architecture_violations(
    graph: DependencyGraph,
    rules: Iterable[ArchitectureRule] | None = None,
) -> bool:
    """
    Returns True if architecture violations exist.
    """

    validator = ArchitectureValidator(
        rules or default_rules()
    )

    return validator.has_violations(graph)