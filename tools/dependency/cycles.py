"""
===============================================================================
PHANTA Dependency Auditor v2.0
Cycle Detection
===============================================================================

Detects circular dependencies in a DependencyGraph.

Responsibilities
----------------
• Traverse the dependency graph
• Detect dependency cycles
• Produce Cycle objects

This module performs NO graph construction.
"""

from __future__ import annotations

from typing import List, Set

from .graph import DependencyGraph
from .models import Cycle


# =============================================================================
# Cycle Detector
# =============================================================================

class CycleDetector:
    """
    Detect circular dependencies using depth-first search.
    """

    def __init__(self, graph: DependencyGraph):

        self.graph = graph

        self._visited: Set[str] = set()

        self._stack: List[str] = []

        self._cycles: List[Cycle] = []

        self._seen_cycles: Set[tuple[str, ...]] = set()

    # -------------------------------------------------------------------------

    def find_cycles(self) -> List[Cycle]:
        """
        Find every circular dependency.
        """

        self._visited.clear()
        self._stack.clear()
        self._cycles.clear()
        self._seen_cycles.clear()

        for module in self.graph.modules():

            if module not in self._visited:

                self._dfs(module)

        return self._cycles

    # -------------------------------------------------------------------------

    def _dfs(
        self,
        module: str,
    ) -> None:

        self._visited.add(module)

        self._stack.append(module)

        for dependency in self.graph.imports_of(module):

            # -------------------------------------------------------------
            # New location
            # -------------------------------------------------------------

            if dependency not in self._visited:

                self._dfs(dependency)

            # -------------------------------------------------------------
            # Back edge → cycle
            # -------------------------------------------------------------

            elif dependency in self._stack:

                start = self._stack.index(dependency)

                cycle = self._stack[start:] + [dependency]

                normalized = self._normalize_cycle(cycle)

                if normalized not in self._seen_cycles:

                    self._seen_cycles.add(normalized)

                    self._cycles.append(
                        Cycle(
                            modules=list(normalized),
                        )
                    )

        self._stack.pop()

    # -------------------------------------------------------------------------

    @staticmethod
    def _normalize_cycle(
        cycle: List[str],
    ) -> tuple[str, ...]:
        """
        Normalize a cycle so duplicates are removed.

        Example

            B → C → A → B

        becomes

            A → B → C → A
        """

        # Remove repeated closing node
        nodes = cycle[:-1]

        if not nodes:
            return tuple()

        # Rotate so smallest name comes first
        start = min(range(len(nodes)), key=lambda i: nodes[i])

        ordered = nodes[start:] + nodes[:start]

        ordered.append(ordered[0])

        return tuple(ordered)


# =============================================================================
# Convenience Functions
# =============================================================================

def find_cycles(
    graph: DependencyGraph,
) -> List[Cycle]:
    """
    Detect circular dependencies.
    """

    return CycleDetector(graph).find_cycles()


def has_cycles(
    graph: DependencyGraph,
) -> bool:
    """
    Returns True if the graph contains cycles.
    """

    return bool(find_cycles(graph))