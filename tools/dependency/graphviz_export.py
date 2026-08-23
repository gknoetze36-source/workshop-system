"""
===============================================================================
PHANTA Dependency Auditor v2.0
Graphviz Exporter
===============================================================================

Exports a DependencyGraph as Graphviz DOT format.

Responsibilities
----------------
• Convert DependencyGraph into DOT format
• Write DOT files to disk

This module performs NO analysis.
"""

from __future__ import annotations

from pathlib import Path

from .graph import DependencyGraph


# =============================================================================
# Graphviz Exporter
# =============================================================================

class GraphvizExporter:
    """
    Exports dependency graphs in Graphviz DOT format.
    """

    def to_dot(
        self,
        graph: DependencyGraph,
        *,
        graph_name: str = "DependencyGraph",
    ) -> str:
        """
        Convert a DependencyGraph into DOT format.
        """

        lines = [
            f'digraph "{graph_name}" {{',
            "",
            "    rankdir=LR;",
            '    bgcolor="white";',
            "",
            '    node [',
            '        shape=box,',
            '        style="rounded,filled",',
            '        fillcolor="lightblue",',
            '        color="gray40",',
            '        fontname="Arial",',
            '        fontsize=10',
            "    ];",
            "",
            '    edge [',
            '        color="gray40",',
            '        arrowsize=0.7',
            "    ];",
            "",
        ]

        # ---------------------------------------------------------------------
        # Nodes
        # ---------------------------------------------------------------------

        for module in graph.modules():

            lines.append(
                f'    "{module}";'
            )

        if graph.module_count:
            lines.append("")

        # ---------------------------------------------------------------------
        # Edges
        # ---------------------------------------------------------------------

        for source, target in graph.edges():

            lines.append(
                f'    "{source}" -> "{target}";'
            )

        lines.append("}")

        return "\n".join(lines)

    # -------------------------------------------------------------------------

    def write(
        self,
        filename: str | Path,
        graph: DependencyGraph,
        *,
        graph_name: str = "DependencyGraph",
    ) -> Path:
        """
        Write the DOT file to disk.
        """

        path = Path(filename)

        path.write_text(
            self.to_dot(
                graph,
                graph_name=graph_name,
            ),
            encoding="utf-8",
        )

        return path


# =============================================================================
# Convenience Functions
# =============================================================================

def export_graphviz(
    filename: str | Path,
    graph: DependencyGraph,
    *,
    graph_name: str = "DependencyGraph",
) -> Path:
    """
    Export a graph as a Graphviz DOT file.
    """

    return GraphvizExporter().write(
        filename,
        graph,
        graph_name=graph_name,
    )


def graph_as_dot(
    graph: DependencyGraph,
    *,
    graph_name: str = "DependencyGraph",
) -> str:
    """
    Return the graph as a DOT string.
    """

    return GraphvizExporter().to_dot(
        graph,
        graph_name=graph_name,
    )