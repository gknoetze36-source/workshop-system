"""
===============================================================================
PHANTA Dependency Auditor v2.0
Duplicate Import Checker
===============================================================================

Detects duplicate import statements within a module.

Responsibilities
----------------
• Detect duplicate imports
• Produce DuplicateImport objects

This module performs NO dependency analysis.
"""

from __future__ import annotations

from collections import defaultdict
from typing import DefaultDict, Iterable, List, Set, Tuple

from .models import DuplicateImport, ImportRecord


# =============================================================================
# Duplicate Import Checker
# =============================================================================

class DuplicateImportChecker:
    """
    Detect duplicate import statements.
    """

    # -------------------------------------------------------------------------

    def find_duplicates(
        self,
        imports: Iterable[ImportRecord],
    ) -> List[DuplicateImport]:
        """
        Find duplicate imports.

        Duplicate means the same module imports the same dependency
        more than once using the same import type.

        Example

            import os
            import os

            from pathlib import Path
            from pathlib import Path
        """

        seen: DefaultDict[str, Set[Tuple[str, str]]] = defaultdict(set)

        duplicates: List[DuplicateImport] = []

        for record in imports:

            key = (
                record.imported_module,
                record.import_type,
            )

            if key in seen[record.module]:

                duplicates.append(
                    DuplicateImport(
                        module=record.module,
                        import_name=record.imported_module,
                        line=record.line,
                        import_type=record.import_type,
                    )
                )

            else:

                seen[record.module].add(key)

        return duplicates

    # -------------------------------------------------------------------------

    def has_duplicates(
        self,
        imports: Iterable[ImportRecord],
    ) -> bool:
        """
        Returns True if duplicate imports exist.
        """

        return bool(
            self.find_duplicates(imports)
        )


# =============================================================================
# Convenience Functions
# =============================================================================

def find_duplicates(
    imports: Iterable[ImportRecord],
) -> List[DuplicateImport]:
    """
    Detect duplicate imports.
    """

    return DuplicateImportChecker().find_duplicates(
        imports
    )


def has_duplicates(
    imports: Iterable[ImportRecord],
) -> bool:
    """
    Returns True if duplicates exist.
    """

    return DuplicateImportChecker().has_duplicates(
        imports
    )