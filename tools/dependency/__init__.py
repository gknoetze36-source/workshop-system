"""
===============================================================================
PHANTA Dependency Auditor
===============================================================================

A modular static analysis toolkit for Python projects.

The dependency auditor is responsible for:

    • Discovering Python source files
    • Parsing imports using the Python AST
    • Building dependency graphs
    • Detecting circular dependencies
    • Detecting duplicate imports
    • Validating project architecture
    • Calculating dependency statistics
    • Producing console, JSON, HTML and Graphviz reports

Package Structure
-----------------

models.py
    Shared dataclasses used throughout the package.

scanner.py
    Recursively scans a project for Python source files.

parser.py
    Parses Python files into structured ImportRecord objects.

graph.py
    Builds and manages the dependency graph.

syntax_checker.py
    Detects Python syntax errors.

import_checker.py
    Performs import validation.

duplicate_checker.py
    Detects duplicate import statements.

cycles.py
    Detects circular dependencies.

architecture.py
    Applies architectural dependency rules.

statistics.py
    Calculates dependency metrics and project health.

reporter.py
    Produces console reports.

json_report.py
    Exports reports as JSON.

html_report.py
    Exports reports as HTML.

graphviz_export.py
    Exports dependency graphs as Graphviz DOT files.

dependency_audit.py
    Main orchestration entry point.

===============================================================================
"""

from .models import (
    SourceFile,
    ImportRecord,
    SyntaxIssue,
    ImportIssue,
    DuplicateImport,
    DependencyNode,
    Cycle,
    ArchitectureViolation,
    AuditSummary,
    AuditReport,
)

__title__ = "PHANTA Dependency Auditor"
__version__ = "2.0.0"
__author__ = "PHANTA Automations"
__license__ = "MIT"

__all__ = [
    "SourceFile",
    "ImportRecord",
    "SyntaxIssue",
    "ImportIssue",
    "DuplicateImport",
    "DependencyNode",
    "Cycle",
    "ArchitectureViolation",
    "AuditSummary",
    "AuditReport",
]