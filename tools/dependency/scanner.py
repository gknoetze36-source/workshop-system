"""
===============================================================================
PHANTA Dependency Auditor v2.0
Project Scanner
===============================================================================

Recursively scans a project directory for Python source files.

Responsibilities
----------------
• Discover Python source files
• Ignore virtual environments and build artifacts
• Produce SourceFile objects

This module performs NO parsing, validation, or dependency analysis.
"""

from __future__ import annotations

from pathlib import Path
from typing import Iterable, List

from .models import SourceFile


# =============================================================================
# Default Ignore Directories
# =============================================================================

DEFAULT_IGNORE = {
    "__pycache__",
    ".git",
    ".github",
    ".venv",
    "venv",
    "env",
    ".pytest_cache",
    ".mypy_cache",
    ".ruff_cache",
    ".idea",
    ".vscode",
    "node_modules",
    "dist",
    "build",
    ".tox",
    ".coverage",
    ".eggs",
    ".svn",
    ".hg",
}


# =============================================================================
# Project Scanner
# =============================================================================

class ProjectScanner:
    """
    Scans a project directory for Python source files.
    """

    def __init__(
        self,
        root: str | Path,
        ignore: Iterable[str] | None = None,
    ):

        self.root = Path(root).resolve()

        self.ignore = set(DEFAULT_IGNORE)

        if ignore:
            self.ignore.update(ignore)

    # -------------------------------------------------------------------------

    def scan(self) -> List[SourceFile]:
        """
        Scan the project and return all discovered Python files.
        """

        files: List[SourceFile] = []

        for path in sorted(self.root.rglob("*.py")):

            if self._should_ignore(path):
                continue

            files.append(
                SourceFile(
                    module=self._module_name(path),
                    path=path,
                )
            )

        return files

    # -------------------------------------------------------------------------

    def _should_ignore(
        self,
        path: Path,
    ) -> bool:
        """
        Returns True if this path should be ignored.
        """

        return any(
            part in self.ignore
            for part in path.parts
        )

    # -------------------------------------------------------------------------

    def _module_name(
        self,
        path: Path,
    ) -> str:
        """
        Convert a file path into a Python module name.

        Example:
            services/booking_service.py

        becomes:
            services.booking_service
        """

        relative = path.relative_to(self.root)

        module = relative.with_suffix("")

        parts = list(module.parts)

        # Remove trailing __init__
        if parts and parts[-1] == "__init__":
            parts.pop()

        return ".".join(parts)

    # -------------------------------------------------------------------------

    def count(self) -> int:
        """
        Returns the number of discovered Python files.
        """

        return len(self.scan())


# =============================================================================
# Convenience Functions
# =============================================================================

def scan_project(
    root: str | Path,
    ignore: Iterable[str] | None = None,
) -> List[SourceFile]:
    """
    Scan a project directory.

    Parameters
    ----------
    root:
        Project root directory.

    ignore:
        Optional additional directories to ignore.

    Returns
    -------
    List[SourceFile]
    """

    return ProjectScanner(
        root=root,
        ignore=ignore,
    ).scan()