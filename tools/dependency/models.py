"""
===============================================================================
PHANTA Dependency Auditor v2.0
Shared Data Models
===============================================================================

All shared dataclasses used by the dependency auditor.

No other module should define its own data structures.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Set


# =============================================================================
# Project Source File
# =============================================================================

@dataclass(slots=True)
class SourceFile:
    """
    Represents one discovered Python source file.
    """

    module: str
    path: Path


# =============================================================================
# Import Record
# =============================================================================

@dataclass(slots=True)
class ImportRecord:
    """
    Represents one import statement.
    """

    module: str
    imported_module: str
    line: int
    import_type: str  # "import" or "from"


# =============================================================================
# Syntax Issue
# =============================================================================

@dataclass(slots=True)
class SyntaxIssue:
    """
    Represents a syntax error found in a source file.
    """

    module: str
    path: Path
    line: int
    message: str


# =============================================================================
# Import Issue
# =============================================================================

@dataclass(slots=True)
class ImportIssue:
    """
    Represents a malformed or invalid import.
    """

    module: str
    line: int
    import_name: str
    issue: str


# =============================================================================
# Duplicate Import
# =============================================================================

@dataclass(slots=True)
class DuplicateImport:
    """
    Represents a duplicate import statement.
    """

    module: str
    import_name: str
    line: int
    import_type: str


# =============================================================================
# Dependency Graph Node
# =============================================================================

@dataclass(slots=True)
class DependencyNode:
    """
    Represents one module inside the dependency graph.
    """

    name: str

    imports: Set[str] = field(default_factory=set)

    imported_by: Set[str] = field(default_factory=set)

    @property
    def import_count(self) -> int:
        return len(self.imports)

    @property
    def incoming_count(self) -> int:
        return len(self.imported_by)


# =============================================================================
# Circular Dependency
# =============================================================================

@dataclass(slots=True)
class Cycle:
    """
    Represents one dependency cycle.
    """

    modules: List[str]

    @property
    def length(self) -> int:
        return len(self.modules)


# =============================================================================
# Architecture Violation
# =============================================================================

@dataclass(slots=True)
class ArchitectureViolation:
    """
    Represents a broken architectural rule.
    """

    source_module: str
    target_module: str
    rule: str
    description: str


# =============================================================================
# Audit Summary
# =============================================================================

@dataclass(slots=True)
class AuditSummary:
    """
    High-level audit metrics.
    """

    modules: int = 0
    imports: int = 0
    cycles: int = 0

    syntax_errors: int = 0
    import_errors: int = 0
    duplicate_imports: int = 0
    architecture_violations: int = 0

    health_score: int = 100


# =============================================================================
# Audit Report
# =============================================================================

@dataclass(slots=True)
class AuditReport:
    """
    Complete dependency audit report.
    """

    summary: AuditSummary

    syntax: List[SyntaxIssue] = field(default_factory=list)

    imports: List[ImportIssue] = field(default_factory=list)

    duplicates: List[DuplicateImport] = field(default_factory=list)

    cycles: List[Cycle] = field(default_factory=list)

    architecture: List[ArchitectureViolation] = field(default_factory=list)


# =============================================================================
# Statistics
# =============================================================================

@dataclass(slots=True)
class Statistics:
    """
    Calculated project dependency statistics.
    """

    modules: int = 0
    dependencies: int = 0

    average_dependencies: float = 0.0

    most_imports_module: str = ""
    most_imports_count: int = 0

    most_depended_module: str = ""
    most_depended_count: int = 0

    cycles: int = 0
    syntax_errors: int = 0
    import_issues: int = 0
    duplicate_imports: int = 0
    architecture_violations: int = 0

    health_score: int = 100