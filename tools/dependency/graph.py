"""
===============================================================================
PHANTA Dependency Auditor v2.0
Dependency Graph
===============================================================================

Builds and manages the project's dependency graph.

Responsibilities
----------------
• Store dependency relationships
• Provide graph queries
• Export graph structure

This module performs NO analysis.
"""

from __future__ import annotations

from typing import Dict, List, Set

from .models import DependencyNode


class DependencyGraph:
    """
    Directed dependency graph.

    Each node represents one Python module.

    module_a
        ├── imports ─────────► module_b
        └── imported_by ◄──── module_c
    """

    def __init__(self) -> None:
        self._nodes: Dict[str, DependencyNode] = {}

    # ------------------------------------------------------------------
    # Node Management
    # ------------------------------------------------------------------

    def add_module(self, module: str) -> DependencyNode:
        """
        Add a module to the graph if it does not already exist.
        """

        if module not in self._nodes:
            self._nodes[module] = DependencyNode(name=module)

        return self._nodes[module]

    # ------------------------------------------------------------------

    def add_dependency(
        self,
        source: str,
        target: str,
    ) -> None:
        """
        Add a directed dependency.

            source -----> target
        """

        source_node = self.add_module(source)
        target_node = self.add_module(target)

        if target not in source_node.imports:
            source_node.imports.add(target)
            target_node.imported_by.add(source)

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------

    def has_module(self, module: str) -> bool:
        return module in self._nodes

    # ------------------------------------------------------------------

    def get_node(
        self,
        module: str,
    ) -> DependencyNode | None:
        return self._nodes.get(module)

    # ------------------------------------------------------------------

    def imports_of(
        self,
        module: str,
    ) -> Set[str]:
        node = self.get_node(module)

        if node is None:
            return set()

        return set(node.imports)

    # ------------------------------------------------------------------

    def imported_by(
        self,
        module: str,
    ) -> Set[str]:
        node = self.get_node(module)

        if node is None:
            return set()

        return set(node.imported_by)

    # ------------------------------------------------------------------

    def modules(self) -> List[str]:
        """
        Return all module names in sorted order.
        """

        return sorted(self._nodes.keys())

    # ------------------------------------------------------------------

    def nodes(self) -> List[DependencyNode]:
        """
        Return all DependencyNode objects.
        """

        return list(self._nodes.values())

    # ------------------------------------------------------------------

    def edges(self) -> List[tuple[str, str]]:
        """
        Return every dependency edge.

        Example

            [
                ("a", "b"),
                ("a", "c"),
                ("b", "os"),
            ]
        """

        edges: List[tuple[str, str]] = []

        for module in self.modules():

            node = self._nodes[module]

            for dependency in sorted(node.imports):
                edges.append(
                    (
                        module,
                        dependency,
                    )
                )

        return edges

    # ------------------------------------------------------------------

    def to_dict(self) -> Dict[str, Set[str]]:
        """
        Export graph as adjacency dictionary.
        """

        return {
            module: set(node.imports)
            for module, node in self._nodes.items()
        }

    # ------------------------------------------------------------------

    def clear(self) -> None:
        """
        Remove all graph data.
        """

        self._nodes.clear()

    # ------------------------------------------------------------------
    # Statistics
    # ------------------------------------------------------------------

    @property
    def module_count(self) -> int:
        return len(self._nodes)

    # ------------------------------------------------------------------

    @property
    def edge_count(self) -> int:
        return sum(
            len(node.imports)
            for node in self._nodes.values()
        )

    # ------------------------------------------------------------------

    @property
    def isolated_modules(self) -> List[str]:
        """
        Modules with no incoming or outgoing dependencies.
        """

        isolated = []

        for node in self._nodes.values():

            if not node.imports and not node.imported_by:
                isolated.append(node.name)

        return sorted(isolated)

    # ------------------------------------------------------------------

    def __contains__(
        self,
        module: str,
    ) -> bool:
        return module in self._nodes

    # ------------------------------------------------------------------

    def __len__(self) -> int:
        return self.module_count

    # ------------------------------------------------------------------

    def __iter__(self):
        return iter(self.modules())

    # ------------------------------------------------------------------

    def __repr__(self) -> str:
        return (
            f"DependencyGraph("
            f"modules={self.module_count}, "
            f"edges={self.edge_count})"
        )