"""Static integrity checks on hand-written SQL.

WHY
---
The franchise -> owner/location migration left a recurring defect: a scope
column was passed twice, so an INSERT supplied one more value than it had
columns. Every affected statement raised at runtime. Five separate instances
were found and fixed:

    services/communication_service.py   12 columns / 13 values
    database/bootstrap.py               12 columns / 13 values (and a duplicated
                                        assignment in the UPDATE, which
                                        PostgreSQL rejects outright)
    services/catalog_service.py          6 columns /  7 placeholders
    services/vehicle_service.py         20 columns / 21 placeholders
    services/reminder_service.py        12 columns / 13 values (x2)
    repositories/booking_repository.py  39 columns / 41 placeholders
    repositories/lead_repository.py      8 columns /  9 placeholders (x2)

Some sat in code paths that are no longer reached, which is precisely why they
survived: nothing exercised them, and nothing checked them. This test checks
them without needing to execute them.

It is deliberately a STATIC check. Executing every statement would need a
fixture per table; parsing them needs nothing and catches the defect the moment
it is written.
"""
import os
import re

import pytest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Directories excluded from the check.
SKIP_DIRS = ("__pycache__", ".git", "node_modules", "migrations")

# The VALUES group is extracted by balancing brackets rather than by regex,
# because a value may itself be a call -- NOW(), COALESCE(...) -- and a lazy
# [^)]* stops at the first inner ")" and miscounts.
INSERT_HEAD_RE = re.compile(
    r"INSERT\s+INTO\s+(\w+)\s*\(([^)]*)\)\s*VALUES\s*\(",
    re.IGNORECASE | re.DOTALL,
)
UPDATE_RE = re.compile(
    r"UPDATE\s+(\w+)\s+SET\s+(.*?)(?:\sWHERE\s|\"\"\")",
    re.IGNORECASE | re.DOTALL,
)


def _split_top_level(text):
    """Split on commas that are not inside brackets or quotes."""
    items, depth, current, quote = [], 0, "", None
    for char in text:
        if quote:
            current += char
            if char == quote:
                quote = None
            continue
        if char in "'\"":
            quote = char
            current += char
            continue
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        if char == "," and depth == 0:
            items.append(current)
            current = ""
        else:
            current += char
    if current.strip():
        items.append(current)
    return [item for item in items if item.strip()]


def _balanced_group(text, open_index):
    """Return the contents of the bracket group starting at open_index."""
    depth, quote, out = 0, None, ""
    for index in range(open_index, len(text)):
        char = text[index]
        if quote:
            out += char
            if char == quote:
                quote = None
            continue
        if char in "'\"":
            quote = char
            out += char
            continue
        if char == "(":
            depth += 1
            if depth == 1:
                continue
        elif char == ")":
            depth -= 1
            if depth == 0:
                return out
        out += char
    return None


def _python_sources():
    for base, dirs, files in os.walk(REPO_ROOT):
        dirs[:] = [d for d in dirs if d not in SKIP_DIRS and not d.startswith(".")]
        if any(skip in base for skip in SKIP_DIRS):
            continue
        for name in files:
            if name.endswith(".py"):
                yield os.path.join(base, name)


def test_insert_columns_match_placeholders():
    """Every INSERT must have as many value slots as it has columns."""
    failures = []
    for path in _python_sources():
        # Test fixtures build statements dynamically; only production code is checked.
        relative = os.path.relpath(path, REPO_ROOT)
        if relative.startswith("tests" + os.sep) or relative.startswith("scripts" + os.sep):
            continue
        try:
            text = open(path, encoding="utf-8").read()
        except OSError:
            continue
        for match in INSERT_HEAD_RE.finditer(text):
            table, columns = match.group(1), match.group(2)
            values = _balanced_group(text, match.end() - 1)
            if values is None:
                continue
            n_columns = len(_split_top_level(columns))
            n_values = len(_split_top_level(values))
            if n_columns != n_values:
                line = text[: match.start()].count("\n") + 1
                failures.append(
                    f"{relative}:{line} INSERT INTO {table}: "
                    f"{n_columns} columns but {n_values} values"
                )
    assert not failures, "INSERT statements with mismatched value counts:\n" + "\n".join(failures)


def test_update_does_not_assign_a_column_twice():
    """PostgreSQL rejects assigning the same column twice in one UPDATE.

    database/bootstrap.py did exactly this (`location_id=%s, location_id=%s`),
    which meant the statement could never run on the production database engine.
    """
    failures = []
    for path in _python_sources():
        relative = os.path.relpath(path, REPO_ROOT)
        if relative.startswith("tests" + os.sep):
            continue
        try:
            text = open(path, encoding="utf-8").read()
        except OSError:
            continue
        for match in UPDATE_RE.finditer(text):
            table, set_clause = match.group(1), match.group(2)
            assigned = re.findall(r"(\w+)\s*=\s*%s", set_clause)
            duplicated = sorted({name for name in assigned if assigned.count(name) > 1})
            if duplicated:
                line = text[: match.start()].count("\n") + 1
                failures.append(f"{relative}:{line} UPDATE {table}: assigns {duplicated} twice")
    assert not failures, "UPDATE statements assigning a column twice:\n" + "\n".join(failures)


def test_no_duplicate_keys_in_dict_literals():
    """A repeated key in a dict literal silently discards the earlier value.

    services/booking_service.py built booking_data with "location_id" twice,
    so the first value was thrown away without any error.
    """
    import ast

    failures = []
    for path in _python_sources():
        relative = os.path.relpath(path, REPO_ROOT)
        try:
            tree = ast.parse(open(path, encoding="utf-8").read())
        except (OSError, SyntaxError):
            continue
        for node in ast.walk(tree):
            if not isinstance(node, ast.Dict):
                continue
            keys = [k.value for k in node.keys
                    if isinstance(k, ast.Constant) and isinstance(k.value, str)]
            duplicated = sorted({k for k in keys if keys.count(k) > 1})
            if duplicated:
                failures.append(f"{relative}:{node.lineno} dict repeats key(s) {duplicated}")
    assert not failures, "dict literals with duplicated keys:\n" + "\n".join(failures)


# ---------------------------------------------------------------------------
# MIGRATIONS vs ROW LEVEL SECURITY
# ---------------------------------------------------------------------------

RLS_PROTECTED_TABLES = (
    "audit_logs", "customers", "bookings", "vehicles", "messages",
    "conversations", "security_events", "security_incidents",
    "legal_acceptances", "service_records", "quotes", "flyer_lady_specials",
)


def test_migrations_touching_rls_tables_disable_rls_first():
    """A migration that changes data in an RLS-protected table must disable RLS.

    Migrations run as the application role, which is subject to
    FORCE ROW LEVEL SECURITY, and no app.location_id is set during a migration.
    The role therefore sees NO rows, and any UPDATE or DELETE silently affects
    zero of them -- with no error.

    This was not theoretical. Migration 0026 repaired orphaned audit_logs rows
    and then added a foreign key. On a real PostgreSQL upgrade with data, the
    repair updated nothing, the orphaned row survived, and PostgreSQL still
    marked the new constraint `convalidated = true` because the validation scan
    also saw no rows. The database then believed a constraint held that did not.

    Setting app.platform_admin is not a fix: the platform policies grant SELECT
    only. The migration owns the table, so it must DISABLE row level security
    for the repair and restore ENABLE + FORCE immediately afterwards.
    """
    import glob

    migrations_dir = os.path.join(REPO_ROOT, "migrations", "versions")
    data_statement = re.compile(
        r"(UPDATE|DELETE\s+FROM)\s+\"?(" + "|".join(RLS_PROTECTED_TABLES) + r")\"?",
        re.IGNORECASE,
    )

    failures = []
    for path in sorted(glob.glob(os.path.join(migrations_dir, "*.py"))):
        try:
            text = open(path, encoding="utf-8").read()
        except OSError:
            continue
        touched = {m.group(2).lower() for m in data_statement.finditer(text)}
        if not touched:
            continue
        name = os.path.basename(path)
        for table in sorted(touched):
            disabled = re.search(
                rf'ALTER TABLE\s+"?{table}"?\s+DISABLE ROW LEVEL SECURITY', text, re.I)
            re_enabled = re.search(
                rf'ALTER TABLE\s+"?{table}"?\s+ENABLE ROW LEVEL SECURITY', text, re.I)
            forced = re.search(
                rf'ALTER TABLE\s+"?{table}"?\s+FORCE ROW LEVEL SECURITY', text, re.I)
            if not disabled:
                failures.append(
                    f"{name}: changes data in RLS-protected `{table}` without "
                    "disabling row level security -- the statement will affect zero rows"
                )
            elif not (re_enabled and forced):
                failures.append(
                    f"{name}: disables row level security on `{table}` but does not "
                    "restore ENABLE + FORCE -- this would leave the table readable "
                    "across tenants"
                )

    assert not failures, "migration / RLS problems:\n" + "\n".join(failures)
