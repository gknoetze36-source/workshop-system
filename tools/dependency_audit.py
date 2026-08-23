###############################################################################
# PHANTA DEPENDENCY AUDITOR v3.0
###############################################################################

from __future__ import annotations

import argparse
import ast
import json
from collections import defaultdict
from dataclasses import dataclass, field
from html import escape
from pathlib import Path
from typing import (
    Any,
    DefaultDict,
    Dict,
    Iterable,
    List,
    Set,
    Tuple,
)

###############################################################################
# CONSTANTS
###############################################################################

VERSION = "3.0"

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
    "build",
    "dist",
    ".tox",
    ".coverage",
    ".eggs",
    ".svn",
    ".hg",
}

###############################################################################
# MODELS
###############################################################################

@dataclass(slots=True)
class SourceFile:
    module: str
    path: Path


@dataclass(slots=True)
class ImportRecord:
    module: str
    imported_module: str
    line: int
    import_type: str


@dataclass(slots=True)
class SyntaxIssue:
    module: str
    path: Path
    line: int
    message: str


@dataclass(slots=True)
class ImportIssue:
    module: str
    line: int
    import_name: str
    issue: str


@dataclass(slots=True)
class DuplicateImport:
    module: str
    import_name: str

    first_line: int
    line: int

    import_type: str


@dataclass(slots=True)
class DependencyNode:

    name: str

    imports: Set[str] = field(default_factory=set)

    imported_by: Set[str] = field(default_factory=set)

    @property
    def import_count(self) -> int:
        return len(self.imports)

    @property
    def incoming_count(self) -> int:
        return len(self.imported_by)


@dataclass(slots=True)
class Cycle:

    modules: List[str]

    @property
    def length(self) -> int:
        return len(self.modules)


@dataclass(slots=True)
class ArchitectureViolation:

    source_module: str
    target_module: str
    rule: str
    description: str


@dataclass(slots=True)
class AuditSummary:

    modules: int = 0
    imports: int = 0
    cycles: int = 0

    syntax_errors: int = 0
    import_errors: int = 0
    duplicate_imports: int = 0
    architecture_violations: int = 0

    health_score: int = 100


@dataclass(slots=True)
class AuditReport:

    summary: AuditSummary

    syntax: List[SyntaxIssue] = field(default_factory=list)

    imports: List[ImportIssue] = field(default_factory=list)

    duplicates: List[DuplicateImport] = field(default_factory=list)

    cycles: List[Cycle] = field(default_factory=list)

    architecture: List[ArchitectureViolation] = field(default_factory=list)


@dataclass(slots=True)
class Statistics:

    modules: int = 0
    dependencies: int = 0

    average_dependencies: float = 0.0

    most_imports_module: str = ""
    most_imports_count: int = 0

    most_depended_module: str = ""
    most_depended_count: int = 0

    syntax_errors: int = 0
    import_issues: int = 0
    duplicate_imports: int = 0
    cycles: int = 0
    architecture_violations: int = 0

    health_score: int = 100


@dataclass(slots=True)
class ArchitectureRule:

    source_prefix: str
    forbidden_prefix: str
    description: str
###############################################################################
# END MODELS
###############################################################################


###############################################################################
# SCANNER
###############################################################################

class ProjectScanner:
    """
    Scans a project directory for Python source files.
    """

    def __init__(
        self,
        ignore_dirs: Iterable[str] | None = None,
    ) -> None:

        self.ignore_dirs = set(ignore_dirs or DEFAULT_IGNORE)

    # -------------------------------------------------------------------------

    def scan(
        self,
        root: str | Path,
    ) -> List[SourceFile]:
        """
        Scan a project and return every Python source file.
        """

        root = Path(root).resolve()

        files: List[SourceFile] = []

        for path in root.rglob("*.py"):

            if self._should_ignore(path):
                continue

            try:
                relative = path.relative_to(root)

            except ValueError:
                continue

            module = self._module_name(relative)

            files.append(
                SourceFile(
                    module=module,
                    path=path,
                )
            )

        files.sort(key=lambda f: f.module)

        return files

    # -------------------------------------------------------------------------

    def _should_ignore(
        self,
        path: Path,
    ) -> bool:
        """
        Returns True if the file should be ignored.
        """

        return any(
            part in self.ignore_dirs
            for part in path.parts
        )

    # -------------------------------------------------------------------------

    @staticmethod
    def _module_name(
        relative_path: Path,
    ) -> str:
        """
        Convert a relative file path into a Python module name.

        Example

            services/booking.py

        becomes

            services.booking
        """

        parts = list(relative_path.with_suffix("").parts)

        if parts and parts[-1] == "__init__":
            parts.pop()

        return ".".join(parts)

###############################################################################
# END SCANNER
###############################################################################



###############################################################################
# PARSER
###############################################################################

class DependencyParser:
    """
    Parses Python source files and extracts module dependencies.
    """

    # -------------------------------------------------------------------------

    def parse_project(
        self,
        files: List[SourceFile],
    ) -> List[ImportRecord]:
        """
        Parse every source file in the project.
        """

        imports: List[ImportRecord] = []

        for source_file in files:

            imports.extend(
                self.parse_file(source_file)
            )

        return imports

    # -------------------------------------------------------------------------

    def parse_file(
        self,
        source_file: SourceFile,
    ) -> List[ImportRecord]:
        """
        Parse a single Python file and return all import statements.
        """

        try:

            source = source_file.path.read_text(
                encoding="utf-8",
            )

            tree = ast.parse(
                source,
                filename=str(source_file.path),
            )

        except (
            OSError,
            UnicodeDecodeError,
            SyntaxError,
        ):

            # Ignore unreadable or syntactically invalid files.
            # Syntax errors are reported separately by SyntaxChecker.
            return []

        imports: List[ImportRecord] = []

        for node in ast.walk(tree):

            # -------------------------------------------------------------
            # import package
            # -------------------------------------------------------------

            if isinstance(node, ast.Import):

                for alias in node.names:

                    imports.append(
                        ImportRecord(
                            module=source_file.module,
                            imported_module=alias.name,
                            line=node.lineno,
                            import_type="import",
                        )
                    )

            # -------------------------------------------------------------
            # from package import name
            # -------------------------------------------------------------

            elif isinstance(node, ast.ImportFrom):

                module = node.module or ""

                if node.level:

                    module = "." * node.level + module

                imports.append(
                    ImportRecord(
                        module=source_file.module,
                        imported_module=module,
                        line=node.lineno,
                        import_type="from",
                    )
                )

        return imports


###############################################################################
# END PARSER
###############################################################################


###############################################################################
# DEPENDENCY GRAPH
###############################################################################

class DependencyGraph:
    """
    Stores module dependency relationships.

    Example

        app
            ├── services.booking
            ├── database

        services.booking
            └── database
    """

    def __init__(
        self,
    ) -> None:

        self._nodes: Dict[str, DependencyNode] = {}

    # -------------------------------------------------------------------------

    def add_module(
        self,
        module: str,
    ) -> DependencyNode:
        """
        Ensure a module exists in the graph.
        """

        if module not in self._nodes:

            self._nodes[module] = DependencyNode(
                name=module,
            )

        return self._nodes[module]

    # -------------------------------------------------------------------------

    def add_dependency(
        self,
        source: str,
        target: str,
    ) -> None:
        """
        Add a dependency edge.

            source ---> target
        """

        source_node = self.add_module(source)

        target_node = self.add_module(target)

        source_node.imports.add(target)

        target_node.imported_by.add(source)

    # -------------------------------------------------------------------------

    def build(
        self,
        imports: List[ImportRecord],
        project_modules: Set[str],
        *,
        include_external: bool = False,
    ) -> None:
        """
        Build the dependency graph.

        By default only project modules are included.

        Set include_external=True to also include third-party and
        standard-library modules.
        """

        self._nodes.clear()

        for record in imports:

            target = record.imported_module.lstrip(".")

            if (
                not include_external
                and target not in project_modules
            ):
                continue

            self.add_dependency(
                record.module,
                target,
            )

    # -------------------------------------------------------------------------

    def node(
        self,
        module: str,
    ) -> DependencyNode | None:
        """
        Return a node by module name.
        """

        return self._nodes.get(module)

    # -------------------------------------------------------------------------

    def modules(
        self,
    ) -> List[str]:
        """
        Return all module names.
        """

        return sorted(
            self._nodes.keys()
        )

    # -------------------------------------------------------------------------

    def nodes(
        self,
    ) -> List[DependencyNode]:
        """
        Return every node.
        """

        return sorted(
            self._nodes.values(),
            key=lambda node: node.name,
        )

    # -------------------------------------------------------------------------

    def imports_of(
        self,
        module: str,
    ) -> Set[str]:
        """
        Return modules imported by the given module.
        """

        node = self.node(module)

        if node is None:
            return set()

        return set(node.imports)

    # -------------------------------------------------------------------------

    def imported_by(
        self,
        module: str,
    ) -> Set[str]:
        """
        Return modules that import the given module.
        """

        node = self.node(module)

        if node is None:
            return set()

        return set(node.imported_by)

    # -------------------------------------------------------------------------

    def edges(
        self,
    ) -> List[Tuple[str, str]]:
        """
        Return every dependency edge.
        """

        edges: List[Tuple[str, str]] = []

        for node in self.nodes():

            for dependency in sorted(node.imports):

                edges.append(
                    (
                        node.name,
                        dependency,
                    )
                )

        return edges

    # -------------------------------------------------------------------------

    @property
    def module_count(
        self,
    ) -> int:
        """
        Number of modules in the graph.
        """

        return len(self._nodes)

    # -------------------------------------------------------------------------

    @property
    def edge_count(
        self,
    ) -> int:
        """
        Number of dependency edges.
        """

        return sum(
            len(node.imports)
            for node in self._nodes.values()
        )

    # -------------------------------------------------------------------------

    def __contains__(
        self,
        module: str,
    ) -> bool:

        return module in self._nodes

    # -------------------------------------------------------------------------

    def __len__(
        self,
    ) -> int:

        return self.module_count

    # -------------------------------------------------------------------------

    def __iter__(
        self,
    ):

        return iter(
            self.modules()
        )


###############################################################################
# END DEPENDENCY GRAPH
###############################################################################


###############################################################################
# SYNTAX CHECKER
###############################################################################

class SyntaxChecker:
    """
    Checks Python files for syntax errors.
    """

    # -------------------------------------------------------------------------

    def check_project(
        self,
        files: List[SourceFile],
    ) -> List[SyntaxIssue]:
        """
        Check every source file in the project.
        """

        issues: List[SyntaxIssue] = []

        for source_file in files:

            issue = self.check_file(source_file)

            if issue is not None:

                issues.append(issue)

        return issues

    # -------------------------------------------------------------------------

    def check_file(
        self,
        source_file: SourceFile,
    ) -> SyntaxIssue | None:
        """
        Validate one Python file.
        """

        try:

            source = source_file.path.read_text(
                encoding="utf-8"
            )

            ast.parse(
                source,
                filename=str(source_file.path),
            )

            return None

        except SyntaxError as error:

            return SyntaxIssue(
                module=source_file.module,
                path=source_file.path,
                line=error.lineno or 0,
                message=error.msg,
            )

        except (
            OSError,
            UnicodeDecodeError,
        ) as error:

            return SyntaxIssue(
                module=source_file.module,
                path=source_file.path,
                line=0,
                message=str(error),
            )

    # -------------------------------------------------------------------------

    def has_errors(
        self,
        files: List[SourceFile],
    ) -> bool:
        """
        Returns True if any syntax errors exist.
        """

        return bool(
            self.check_project(files)
        )


###############################################################################
# END SYNTAX CHECKER
###############################################################################

###############################################################################
# IMPORT CHECKER
###############################################################################

class ImportChecker:
    """
    Validates parsed import statements.

    This checker does NOT verify whether a module exists.
    It only validates the syntax and consistency of the
    parsed ImportRecord objects.
    """

    # -------------------------------------------------------------------------

    def check_project(
        self,
        imports: List[ImportRecord],
    ) -> List[ImportIssue]:
        """
        Validate every parsed import.
        """

        issues: List[ImportIssue] = []

        for record in imports:

            issues.extend(
                self.check_import(record)
            )

        return issues

    # -------------------------------------------------------------------------

    def check_import(
        self,
        record: ImportRecord,
    ) -> List[ImportIssue]:
        """
        Validate a single import.
        """

        issues: List[ImportIssue] = []

        module = record.imported_module.strip()

        # -------------------------------------------------------------
        # Empty import
        # -------------------------------------------------------------

        if not module:

            issues.append(
                ImportIssue(
                    module=record.module,
                    line=record.line,
                    import_name=record.imported_module,
                    issue="Empty import target.",
                )
            )

            return issues

        # -------------------------------------------------------------
        # Self import
        # -------------------------------------------------------------

        if module == record.module:

            issues.append(
                ImportIssue(
                    module=record.module,
                    line=record.line,
                    import_name=module,
                    issue="Module imports itself.",
                )
            )

        # -------------------------------------------------------------
        # Trailing dot
        # -------------------------------------------------------------

        if module.endswith("."):

            issues.append(
                ImportIssue(
                    module=record.module,
                    line=record.line,
                    import_name=module,
                    issue="Import ends with '.'.",
                )
            )

        # -------------------------------------------------------------
        # Consecutive dots
        #
        # Ignore valid relative imports:
        #
        # .
        # ..
        # ...
        # -------------------------------------------------------------

        if (
            ".." in module
            and not module.startswith(".")
        ):

            issues.append(
                ImportIssue(
                    module=record.module,
                    line=record.line,
                    import_name=module,
                    issue="Suspicious consecutive dots.",
                )
            )

        return issues

    # -------------------------------------------------------------------------

    def has_issues(
        self,
        imports: List[ImportRecord],
    ) -> bool:
        """
        Returns True if any import issues exist.
        """

        return bool(
            self.check_project(imports)
        )


###############################################################################
# END IMPORT CHECKER
###############################################################################

###############################################################################
# DUPLICATE IMPORT CHECKER
###############################################################################

class DuplicateImportChecker:
    """
    Detect duplicate module imports.

    A duplicate is defined as importing the same module more than once
    within the same source module using the same import style.

    Examples

        import database
        import database

    or

        from services import booking
        from services import booking

    The first occurrence is treated as the original import.
    Every subsequent occurrence is reported as a duplicate.
    """

    # -------------------------------------------------------------------------

    def check_project(
        self,
        imports: List[ImportRecord],
    ) -> List[DuplicateImport]:
        """
        Detect duplicate imports across the project.
        """

        duplicates: List[DuplicateImport] = []

        # module -> (import_type, imported_module) -> first line seen
        seen: Dict[
            str,
            Dict[Tuple[str, str], int],
        ] = defaultdict(dict)

        records = sorted(
            imports,
            key=lambda record: (
                record.module,
                record.line,
            ),
        )

        for record in records:

            key = (
                record.import_type,
                record.imported_module,
            )

            module_seen = seen[record.module]

            if key not in module_seen:

                module_seen[key] = record.line
                continue

            duplicates.append(

                DuplicateImport(

                    module=record.module,

                    import_name=record.imported_module,

                    line=record.line,

                    first_line=module_seen[key],

                    import_type=record.import_type,

                )

            )

        return duplicates

    # -------------------------------------------------------------------------

    def duplicate_count(
        self,
        imports: List[ImportRecord],
    ) -> int:
        """
        Return the number of duplicate imports.
        """

        return len(
            self.check_project(imports)
        )

    # -------------------------------------------------------------------------

    def has_duplicates(
        self,
        imports: List[ImportRecord],
    ) -> bool:
        """
        Return True if duplicate imports exist.
        """

        return (
            self.duplicate_count(imports)
            > 0
        )


###############################################################################
# END DUPLICATE IMPORT CHECKER
###############################################################################


###############################################################################
# CYCLE DETECTOR
###############################################################################

class CycleDetector:
    """
    Detects circular dependencies in a DependencyGraph.
    """

    def __init__(self) -> None:

        self._visited: Set[str] = set()

        self._stack: List[str] = []

        self._cycles: List[Cycle] = []

        self._seen: Set[Tuple[str, ...]] = set()

    # -------------------------------------------------------------------------

    def find_cycles(
        self,
        graph: DependencyGraph,
    ) -> List[Cycle]:
        """
        Find every circular dependency in the graph.
        """

        self._visited.clear()
        self._stack.clear()
        self._cycles.clear()
        self._seen.clear()

        for module in graph.modules():

            if module not in self._visited:

                self._visit(
                    graph,
                    module,
                )

        return list(self._cycles)

    # -------------------------------------------------------------------------

    def has_cycles(
        self,
        graph: DependencyGraph,
    ) -> bool:
        """
        Returns True if the graph contains circular dependencies.
        """

        return bool(
            self.find_cycles(graph)
        )

    # -------------------------------------------------------------------------

    def _visit(
        self,
        graph: DependencyGraph,
        module: str,
    ) -> None:

        self._visited.add(module)

        self._stack.append(module)

        for dependency in graph.imports_of(module):

            # -------------------------------------------------------------
            # Visit new node
            # -------------------------------------------------------------

            if dependency not in self._visited:

                self._visit(
                    graph,
                    dependency,
                )

            # -------------------------------------------------------------
            # Back edge detected
            # -------------------------------------------------------------

            elif dependency in self._stack:

                start = self._stack.index(
                    dependency
                )

                cycle = (
                    self._stack[start:]
                    + [dependency]
                )

                normalized = self._normalize(
                    cycle
                )

                if normalized not in self._seen:

                    self._seen.add(
                        normalized
                    )

                    self._cycles.append(
                        Cycle(
                            modules=list(normalized)
                        )
                    )

        self._stack.pop()

    # -------------------------------------------------------------------------

    @staticmethod
    def _normalize(
        cycle: List[str],
    ) -> Tuple[str, ...]:
        """
        Normalize a cycle so duplicate reports are removed.

        Example

            B -> C -> A -> B

        becomes

            A -> B -> C -> A
        """

        nodes = cycle[:-1]

        if not nodes:

            return tuple()

        start = min(
            range(len(nodes)),
            key=lambda i: nodes[i],
        )

        ordered = (
            nodes[start:]
            + nodes[:start]
        )

        ordered.append(
            ordered[0]
        )

        return tuple(ordered)


###############################################################################
# END CYCLE DETECTOR
###############################################################################

###############################################################################
# ARCHITECTURE VALIDATOR
###############################################################################

class ArchitectureValidator:
    """
    Validates dependency relationships against architecture rules.
    """

    def __init__(
        self,
        rules: List[ArchitectureRule] | None = None,
    ) -> None:

        self.rules = rules or self.default_rules()

    # -------------------------------------------------------------------------

    @staticmethod
    def default_rules() -> List[ArchitectureRule]:
        """
        Default architecture rules.

        Rules match module path segments rather than exact module names.

        Example

            routes.admin.users

        contains the segment "routes"

            services.database.postgres

        contains the segment "database"
        """

        return [

            ArchitectureRule(
                source_prefix="routes",
                forbidden_prefix="database",
                description="Routes should use services instead of accessing the database directly.",
            ),

            ArchitectureRule(
                source_prefix="api",
                forbidden_prefix="database",
                description="API modules should not access the database directly.",
            ),

            ArchitectureRule(
                source_prefix="ui",
                forbidden_prefix="database",
                description="UI modules should not access the database directly.",
            ),

            ArchitectureRule(
                source_prefix="tests",
                forbidden_prefix="app",
                description="Tests should avoid importing the application entry point.",
            ),
        ]

    # -------------------------------------------------------------------------

    @staticmethod
    def _contains_segment(
        module: str,
        segment: str,
    ) -> bool:
        """
        Returns True if the module path contains the given segment.

        Examples

            services.database
                -> contains "database"

            api.v1.users
                -> contains "api"

            routes.admin.users
                -> contains "routes"
        """

        return segment in module.split(".")

    # -------------------------------------------------------------------------

    def validate(
        self,
        graph: DependencyGraph,
    ) -> List[ArchitectureViolation]:
        """
        Validate the dependency graph.
        """

        violations: List[ArchitectureViolation] = []

        for source, target in graph.edges():

            for rule in self.rules:

                if (
                    self._contains_segment(
                        source,
                        rule.source_prefix,
                    )
                    and
                    self._contains_segment(
                        target,
                        rule.forbidden_prefix,
                    )
                ):

                    violations.append(

                        ArchitectureViolation(

                            source_module=source,

                            target_module=target,

                            rule=(
                                f"{rule.source_prefix}"
                                " -> "
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

        return bool(
            self.validate(graph)
        )


###############################################################################
# END ARCHITECTURE VALIDATOR
###############################################################################


###############################################################################
# STATISTICS
###############################################################################

class StatisticsCalculator:
    """
    Calculates project statistics from analysis results.
    """

    # -------------------------------------------------------------------------

    def calculate(
        self,
        graph: DependencyGraph,
        *,
        syntax_errors: List[SyntaxIssue],
        import_issues: List[ImportIssue],
        duplicate_imports: List[DuplicateImport],
        cycles: List[Cycle],
        architecture_violations: List[ArchitectureViolation],
    ) -> Statistics:
        """
        Calculate project statistics.
        """

        stats = Statistics()

        # ---------------------------------------------------------------------
        # Basic Graph Statistics
        # ---------------------------------------------------------------------

        stats.modules = graph.module_count

        stats.dependencies = graph.edge_count

        if stats.modules:

            stats.average_dependencies = (
                stats.dependencies / stats.modules
            )

        # ---------------------------------------------------------------------
        # Module With Most Imports
        # ---------------------------------------------------------------------

        for node in graph.nodes():

            if node.import_count > stats.most_imports_count:

                stats.most_imports_count = node.import_count

                stats.most_imports_module = node.name

            if node.incoming_count > stats.most_depended_count:

                stats.most_depended_count = node.incoming_count

                stats.most_depended_module = node.name

        # ---------------------------------------------------------------------
        # Analysis Counts
        # ---------------------------------------------------------------------

        stats.syntax_errors = len(
            syntax_errors
        )

        stats.import_issues = len(
            import_issues
        )

        stats.duplicate_imports = len(
            duplicate_imports
        )

        stats.cycles = len(
            cycles
        )

        stats.architecture_violations = len(
            architecture_violations
        )

        # ---------------------------------------------------------------------
        # Health Score
        # ---------------------------------------------------------------------

        score = 100

        score -= stats.syntax_errors * 10

        score -= stats.cycles * 5

        score -= stats.architecture_violations * 5

        score -= stats.import_issues * 2

        score -= stats.duplicate_imports

        stats.health_score = max(
            0,
            score,
        )

        return stats

    # -------------------------------------------------------------------------

    def summary(
        self,
        statistics: Statistics,
    ) -> AuditSummary:
        """
        Convert Statistics into an AuditSummary.
        """

        return AuditSummary(

            modules=statistics.modules,

            imports=statistics.dependencies,

            cycles=statistics.cycles,

            syntax_errors=statistics.syntax_errors,

            import_errors=statistics.import_issues,

            duplicate_imports=statistics.duplicate_imports,

            architecture_violations=(
                statistics.architecture_violations
            ),

            health_score=statistics.health_score,
        )


###############################################################################
# END STATISTICS
###############################################################################

###############################################################################
# CONSOLE REPORTER
###############################################################################

class ConsoleReporter:
    """
    Produces a human-readable dependency audit report.
    """

    # -------------------------------------------------------------------------

    def generate(
        self,
        report: AuditReport,
        statistics: Statistics,
    ) -> str:
        """
        Generate a formatted report.
        """

        lines: List[str] = []

        divider = "=" * 78
        section = "-" * 78

        # ---------------------------------------------------------------------
        # Header
        # ---------------------------------------------------------------------

        lines.append(divider)
        lines.append(
            f"PHANTA Dependency Auditor v{VERSION}"
        )
        lines.append(divider)
        lines.append("")

        # ---------------------------------------------------------------------
        # Summary
        # ---------------------------------------------------------------------

        grade = self._grade(
            statistics.health_score
        )

        lines.append("SUMMARY")
        lines.append(section)

        lines.append(
            f"Modules                  : {statistics.modules}"
        )

        lines.append(
            f"Dependencies             : {statistics.dependencies}"
        )

        lines.append(
            f"Average Dependencies     : "
            f"{statistics.average_dependencies:.2f}"
        )

        lines.append(
            f"Health Score             : "
            f"{statistics.health_score}/100 ({grade})"
        )

        lines.append("")

        # ---------------------------------------------------------------------
        # Analysis
        # ---------------------------------------------------------------------

        lines.append("ANALYSIS")
        lines.append(section)

        lines.append(
            f"Syntax Errors            : {statistics.syntax_errors}"
        )

        lines.append(
            f"Import Issues            : {statistics.import_issues}"
        )

        lines.append(
            f"Duplicate Imports        : {statistics.duplicate_imports}"
        )

        lines.append(
            f"Circular Dependencies    : {statistics.cycles}"
        )

        lines.append(
            f"Architecture Violations  : "
            f"{statistics.architecture_violations}"
        )

        lines.append("")

        # ---------------------------------------------------------------------
        # Dependency Metrics
        # ---------------------------------------------------------------------

        lines.append("DEPENDENCY METRICS")
        lines.append(section)

        lines.append(
            f"Most Imports             : "
            f"{statistics.most_imports_module} "
            f"({statistics.most_imports_count})"
        )

        lines.append(
            f"Most Imported By         : "
            f"{statistics.most_depended_module} "
            f"({statistics.most_depended_count})"
        )

        lines.append("")

        # ---------------------------------------------------------------------
        # Syntax Errors
        # ---------------------------------------------------------------------

        if report.syntax:

            lines.append("SYNTAX ERRORS")
            lines.append(section)

            for issue in report.syntax:

                lines.append(
                    f"{issue.module}:{issue.line}"
                )

                lines.append(
                    f"    {issue.message}"
                )

            lines.append("")

        # ---------------------------------------------------------------------
        # Import Issues
        # ---------------------------------------------------------------------

        if report.imports:

            lines.append("IMPORT ISSUES")
            lines.append(section)

            for issue in report.imports:

                lines.append(
                    f"{issue.module}:{issue.line}"
                )

                lines.append(
                    f"    {issue.issue}"
                )

            lines.append("")

        # ---------------------------------------------------------------------
        # Duplicate Imports
        # ---------------------------------------------------------------------

        if report.duplicates:

            lines.append("DUPLICATE IMPORTS")
            lines.append(section)

            for duplicate in report.duplicates:

                lines.append(
                    f"{duplicate.module}:{duplicate.line}"
                )

                lines.append(
                    f"    {duplicate.import_name}"
                )

            lines.append("")

        # ---------------------------------------------------------------------
        # Circular Dependencies
        # ---------------------------------------------------------------------

        if report.cycles:

            lines.append("CIRCULAR DEPENDENCIES")
            lines.append(section)

            for cycle in report.cycles:

                lines.append(
                    "    "
                    + " -> ".join(cycle.modules)
                )

            lines.append("")

        # ---------------------------------------------------------------------
        # Architecture Violations
        # ---------------------------------------------------------------------

        if report.architecture:

            lines.append(
                "ARCHITECTURE VIOLATIONS"
            )

            lines.append(section)

            for violation in report.architecture:

                lines.append(
                    violation.source_module
                )

                lines.append(
                    f"    imports {violation.target_module}"
                )

                lines.append(
                    f"    Rule: {violation.rule}"
                )

                lines.append(
                    f"    {violation.description}"
                )

                lines.append("")

        # ---------------------------------------------------------------------
        # Recommendations
        # ---------------------------------------------------------------------

        recommendations = self._recommendations(
            statistics
        )

        if recommendations:

            lines.append("RECOMMENDATIONS")
            lines.append(section)

            for recommendation in recommendations:

                lines.append(
                    f"- {recommendation}"
                )

            lines.append("")

        return "\n".join(lines)

    # -------------------------------------------------------------------------

    @staticmethod
    def _grade(
        score: int,
    ) -> str:

        if score >= 90:
            return "A"

        if score >= 80:
            return "B"

        if score >= 70:
            return "C"

        if score >= 50:
            return "D"

        return "F"

    # -------------------------------------------------------------------------

    @staticmethod
    def _recommendations(
        statistics: Statistics,
    ) -> List[str]:

        recommendations: List[str] = []

        if statistics.syntax_errors:

            recommendations.append(
                "Fix syntax errors first."
            )

        if statistics.cycles:

            recommendations.append(
                "Break circular dependencies."
            )

        if statistics.duplicate_imports:

            recommendations.append(
                "Remove duplicate imports."
            )

        if statistics.architecture_violations:

            recommendations.append(
                "Review architecture violations."
            )

        if (
            not statistics.syntax_errors
            and
            not statistics.import_issues
        ):

            recommendations.append(
                "No syntax or import issues detected."
            )

        return recommendations

    # -------------------------------------------------------------------------

    def print(
        self,
        report: AuditReport,
        statistics: Statistics,
    ) -> None:
        """
        Print the report.
        """

        print(
            self.generate(
                report,
                statistics,
            )
        )


###############################################################################
# END CONSOLE REPORTER
###############################################################################


###############################################################################
# JSON REPORT EXPORTER
###############################################################################

class JsonReportExporter:
    """
    Exports dependency audit results as JSON.
    """

    # -------------------------------------------------------------------------

    def to_dict(
        self,
        report: AuditReport,
        statistics: Statistics,
    ) -> Dict[str, Any]:
        """
        Convert an audit report into a dictionary.
        """

        return {

            "version": VERSION,

            "summary": {

                "modules": statistics.modules,

                "dependencies": statistics.dependencies,

                "average_dependencies":
                    statistics.average_dependencies,

                "health_score":
                    statistics.health_score,
            },

            "analysis": {

                "syntax_errors":
                    statistics.syntax_errors,

                "import_issues":
                    statistics.import_issues,

                "duplicate_imports":
                    statistics.duplicate_imports,

                "cycles":
                    statistics.cycles,

                "architecture_violations":
                    statistics.architecture_violations,
            },

            "dependency_metrics": {

                "most_imports_module":
                    statistics.most_imports_module,

                "most_imports_count":
                    statistics.most_imports_count,

                "most_depended_module":
                    statistics.most_depended_module,

                "most_depended_count":
                    statistics.most_depended_count,
            },

            "syntax": [

                {

                    "module": issue.module,

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

                    "type": duplicate.import_type,

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

                    "source_module":
                        violation.source_module,

                    "target_module":
                        violation.target_module,

                    "rule":
                        violation.rule,

                    "description":
                        violation.description,

                }

                for violation in report.architecture

            ],
        }

    # -------------------------------------------------------------------------

    def export(
        self,
        filename: str | Path,
        report: AuditReport,
        statistics: Statistics,
    ) -> Path:
        """
        Export the audit report as JSON.
        """

        path = Path(filename)

        path.write_text(

            json.dumps(

                self.to_dict(

                    report,

                    statistics,

                ),

                indent=4,

                sort_keys=False,

            ),

            encoding="utf-8",

        )

        return path


###############################################################################
# END JSON REPORT EXPORTER
###############################################################################

###############################################################################
# HTML REPORT EXPORTER
###############################################################################

class HtmlReportExporter:
    """
    Exports the dependency audit as an HTML report.
    """

    # -------------------------------------------------------------------------

    def export(
        self,
        filename: str | Path,
        report: AuditReport,
        statistics: Statistics,
    ) -> Path:
        """
        Export an HTML report.
        """

        path = Path(filename)

        html = f"""<!DOCTYPE html>
<html lang="en">

<head>

<meta charset="utf-8">

<title>PHANTA Dependency Report</title>

<style>

body {{
    font-family: Arial, Helvetica, sans-serif;
    margin:40px;
    background:#f8f9fa;
}}

h1 {{
    color:#1f2937;
}}

h2 {{
    margin-top:35px;
}}

table {{
    border-collapse:collapse;
    width:100%;
}}

th, td {{
    border:1px solid #cccccc;
    padding:8px;
    text-align:left;
}}

th {{
    background:#eeeeee;
}}

.good {{
    color:green;
}}

.warning {{
    color:darkorange;
}}

.bad {{
    color:red;
}}

pre {{
    background:#efefef;
    padding:10px;
}}

</style>

</head>

<body>

<h1>PHANTA Dependency Auditor</h1>

<p>

<b>Version:</b> {VERSION}<br>

<b>Health Score:</b>

<span class="{self._health_class(statistics.health_score)}">

{statistics.health_score}/100

</span>

</p>

<h2>Summary</h2>

<table>

<tr><th>Metric</th><th>Value</th></tr>

<tr><td>Modules</td><td>{statistics.modules}</td></tr>

<tr><td>Dependencies</td><td>{statistics.dependencies}</td></tr>

<tr><td>Average Dependencies</td>
<td>{statistics.average_dependencies:.2f}</td></tr>

<tr><td>Syntax Errors</td>
<td>{statistics.syntax_errors}</td></tr>

<tr><td>Import Issues</td>
<td>{statistics.import_issues}</td></tr>

<tr><td>Duplicate Imports</td>
<td>{statistics.duplicate_imports}</td></tr>

<tr><td>Cycles</td>
<td>{statistics.cycles}</td></tr>

<tr><td>Architecture Violations</td>
<td>{statistics.architecture_violations}</td></tr>

</table>

<h2>Most Connected Modules</h2>

<table>

<tr>
<th>Metric</th>
<th>Module</th>
<th>Count</th>
</tr>

<tr>

<td>Most Imports</td>

<td>{escape(statistics.most_imports_module)}</td>

<td>{statistics.most_imports_count}</td>

</tr>

<tr>

<td>Most Imported</td>

<td>{escape(statistics.most_depended_module)}</td>

<td>{statistics.most_depended_count}</td>

</tr>

</table>

<h2>Syntax Errors</h2>

<pre>

{self._syntax(report)}

</pre>

<h2>Import Issues</h2>

<pre>

{self._imports(report)}

</pre>

<h2>Duplicate Imports</h2>

<pre>

{self._duplicates(report)}

</pre>

<h2>Circular Dependencies</h2>

<pre>

{self._cycles(report)}

</pre>

<h2>Architecture Violations</h2>

<pre>

{self._architecture(report)}

</pre>

</body>

</html>
"""

        path.write_text(
            html,
            encoding="utf-8",
        )

        return path

    # -------------------------------------------------------------------------

    @staticmethod
    def _health_class(
        score: int,
    ) -> str:

        if score >= 90:
            return "good"

        if score >= 70:
            return "warning"

        return "bad"

    # -------------------------------------------------------------------------

    @staticmethod
    def _syntax(
        report: AuditReport,
    ) -> str:

        if not report.syntax:
            return "None"

        return "\n".join(
            f"{i.module}:{i.line} - {i.message}"
            for i in report.syntax
        )

    # -------------------------------------------------------------------------

    @staticmethod
    def _imports(
        report: AuditReport,
    ) -> str:

        if not report.imports:
            return "None"

        return "\n".join(
            f"{i.module}:{i.line} - {i.issue}"
            for i in report.imports
        )

    # -------------------------------------------------------------------------

    @staticmethod
    def _duplicates(
        report: AuditReport,
    ) -> str:

        if not report.duplicates:
            return "None"

        return "\n".join(
            f"{d.module}:{d.line} - {d.import_name}"
            for d in report.duplicates
        )

    # -------------------------------------------------------------------------

    @staticmethod
    def _cycles(
        report: AuditReport,
    ) -> str:

        if not report.cycles:
            return "None"

        return "\n".join(
            " -> ".join(c.modules)
            for c in report.cycles
        )

    # -------------------------------------------------------------------------

    @staticmethod
    def _architecture(
        report: AuditReport,
    ) -> str:

        if not report.architecture:
            return "None"

        return "\n".join(
            (
                f"{v.source_module} -> "
                f"{v.target_module} "
                f"({v.rule})"
            )
            for v in report.architecture
        )


###############################################################################
# END HTML REPORT EXPORTER
###############################################################################

###############################################################################
# GRAPHVIZ EXPORTER
###############################################################################

class GraphvizExporter:
    """
    Exports the dependency graph as a Graphviz DOT file.
    """

    # -------------------------------------------------------------------------

    def export(
        self,
        filename: str | Path,
        graph: DependencyGraph,
    ) -> Path:
        """
        Export the dependency graph to a Graphviz DOT file.
        """

        path = Path(filename)

        path.write_text(
            self.to_string(graph),
            encoding="utf-8",
        )

        return path

    # -------------------------------------------------------------------------

    def to_string(
        self,
        graph: DependencyGraph,
    ) -> str:
        """
        Return the Graphviz representation as a string.
        """

        lines: List[str] = [

            "digraph Dependencies {",

            "    rankdir=LR;",

            '    node [shape=box];',

            "",
        ]

        # ---------------------------------------------------------------------
        # Nodes
        # ---------------------------------------------------------------------

        for module in graph.modules():

            lines.append(
                f'    "{module}";'
            )

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


###############################################################################
# END GRAPHVIZ EXPORTER
###############################################################################


###############################################################################
# DEPENDENCY AUDITOR
###############################################################################

class DependencyAuditor:
    """
    Coordinates the complete dependency audit.
    """

    def __init__(
        self,
    ) -> None:

        self.scanner = ProjectScanner()

        self.parser = DependencyParser()

        self.graph = DependencyGraph()

        self.syntax_checker = SyntaxChecker()

        self.import_checker = ImportChecker()

        self.duplicate_checker = DuplicateImportChecker()

        self.cycle_detector = CycleDetector()

        self.architecture_validator = ArchitectureValidator()

        self.statistics = StatisticsCalculator()

    # -------------------------------------------------------------------------

    def audit(
        self,
        root: str | Path,
    ) -> tuple[
        AuditReport,
        Statistics,
        DependencyGraph,
    ]:
        """
        Run a complete dependency audit.
        """

        # ---------------------------------------------------------------------
        # Scan Project
        # ---------------------------------------------------------------------

        files = self.scanner.scan(root)

        project_modules = {

            source.module

            for source in files

        }

        # ---------------------------------------------------------------------
        # Parse Imports
        # ---------------------------------------------------------------------

        imports = self.parser.parse_project(
            files
        )

        # ---------------------------------------------------------------------
        # Build Dependency Graph
        # ---------------------------------------------------------------------

        self.graph.build(

            imports,

            project_modules,

        )

        # ---------------------------------------------------------------------
        # Run Analyses
        # ---------------------------------------------------------------------

        syntax = self.syntax_checker.check_project(
            files
        )

        import_issues = self.import_checker.check_project(
            imports
        )

        duplicates = self.duplicate_checker.check_project(
            imports
        )

        cycles = self.cycle_detector.find_cycles(
            self.graph
        )

        architecture = (
            self.architecture_validator.validate(
                self.graph
            )
        )

        # ---------------------------------------------------------------------
        # Calculate Statistics
        # ---------------------------------------------------------------------

        stats = self.statistics.calculate(

            self.graph,

            syntax_errors=syntax,

            import_issues=import_issues,

            duplicate_imports=duplicates,

            cycles=cycles,

            architecture_violations=architecture,

        )

        # ---------------------------------------------------------------------
        # Build Report
        # ---------------------------------------------------------------------

        report = AuditReport(

            summary=self.statistics.summary(
                stats
            ),

            syntax=syntax,

            imports=import_issues,

            duplicates=duplicates,

            cycles=cycles,

            architecture=architecture,

        )

        return (

            report,

            stats,

            self.graph,

        )


###############################################################################
# END DEPENDENCY AUDITOR
###############################################################################



###############################################################################
# MAIN
###############################################################################

def main() -> None:
    """
    Command-line entry point.
    """

    parser = argparse.ArgumentParser(
        description="PHANTA Dependency Auditor"
    )

    parser.add_argument(
        "project",
        nargs="?",
        default=".",
        help="Project directory to analyse.",
    )

    parser.add_argument(
        "--json",
        metavar="FILE",
        help="Write JSON report.",
    )

    parser.add_argument(
        "--html",
        metavar="FILE",
        help="Write HTML report.",
    )

    parser.add_argument(
        "--dot",
        metavar="FILE",
        help="Write Graphviz DOT file.",
    )

    args = parser.parse_args()

    # -------------------------------------------------------------------------
    # Run Audit
    # -------------------------------------------------------------------------

    auditor = DependencyAuditor()

    report, statistics, graph = auditor.audit(
        args.project
    )

    # -------------------------------------------------------------------------
    # Console Report
    # -------------------------------------------------------------------------

    ConsoleReporter().print(
        report,
        statistics,
    )

    # -------------------------------------------------------------------------
    # Optional Exports
    # -------------------------------------------------------------------------

    if args.json:

        JsonReportExporter().export(
            args.json,
            report,
            statistics,
        )

        print(
            f"\nJSON report written to: {args.json}"
        )

    if args.html:

        HtmlReportExporter().export(
            args.html,
            report,
            statistics,
        )

        print(
            f"HTML report written to: {args.html}"
        )

    if args.dot:

        GraphvizExporter().export(
            args.dot,
            graph,
        )

        print(
            f"Graphviz file written to: {args.dot}"
        )


# -----------------------------------------------------------------------------


if __name__ == "__main__":

    main()


###############################################################################
# END FILE
###############################################################################



