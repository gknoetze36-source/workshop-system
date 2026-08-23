import os
import re
import glob
import psycopg2

DB_URL = os.environ["DATABASE_URL"]

# ------------------------------------------------------------
# 1. Extract table references from Python source
# ------------------------------------------------------------

patterns = [
    r'\bFROM\s+["`]?([a-zA-Z_][a-zA-Z0-9_]*)["`]?',
    r'\bJOIN\s+["`]?([a-zA-Z_][a-zA-Z0-9_]*)["`]?',
    r'\bINSERT\s+INTO\s+["`]?([a-zA-Z_][a-zA-Z0-9_]*)["`]?',
    r'\bUPDATE\s+["`]?([a-zA-Z_][a-zA-Z0-9_]*)["`]?',
    r'\bDELETE\s+FROM\s+["`]?([a-zA-Z_][a-zA-Z0-9_]*)["`]?',
]

ignored = {
    "select", "where", "set", "values", "returning",
    "current", "information_schema", "pg_roles",
    "pg_tables", "pg_class", "pg_namespace",
    "pg_constraint", "pg_trigger", "pg_policies",
    "alembic_version",
}

code_tables = set()

for path in glob.glob("**/*.py", recursive=True):
    # Skip virtual environments and caches
    if any(x in path.replace("\\", "/").split("/") for x in
           [".venv", "venv", "__pycache__", ".git"]):
        continue

    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            text = f.read()
    except Exception:
        continue

    for pattern in patterns:
        for match in re.finditer(pattern, text, re.IGNORECASE):
            table = match.group(1).lower()

            if table not in ignored:
                code_tables.add(table)


# ------------------------------------------------------------
# 2. Connect to PostgreSQL
# ------------------------------------------------------------

conn = psycopg2.connect(DB_URL)
cur = conn.cursor()


# ------------------------------------------------------------
# 3. Actual database tables
# ------------------------------------------------------------

cur.execute("""
SELECT
    schemaname,
    tablename,
    tableowner
FROM pg_tables
WHERE schemaname = 'public'
ORDER BY tablename
""")

db_tables = cur.fetchall()

db_table_names = {
    row[1].lower()
    for row in db_tables
}


# ------------------------------------------------------------
# 4. RLS status
# ------------------------------------------------------------

cur.execute("""
SELECT
    n.nspname,
    c.relname,
    c.relrowsecurity,
    c.relforcerowsecurity
FROM pg_class c
JOIN pg_namespace n
    ON n.oid = c.relnamespace
WHERE n.nspname = 'public'
  AND c.relkind = 'r'
ORDER BY c.relname
""")

rls_rows = cur.fetchall()

rls = {
    row[1].lower(): (row[2], row[3])
    for row in rls_rows
}


# ------------------------------------------------------------
# 5. PHANTA APP privileges
# ------------------------------------------------------------

cur.execute("""
SELECT
    table_name,
    privilege_type
FROM information_schema.role_table_grants
WHERE grantee = 'phanta_app'
  AND table_schema = 'public'
ORDER BY table_name, privilege_type
""")

grant_rows = cur.fetchall()

privileges = {}

for table, privilege in grant_rows:
    privileges.setdefault(table.lower(), set()).add(privilege.upper())


# ------------------------------------------------------------
# 6. Schema privilege
# ------------------------------------------------------------

cur.execute("""
SELECT has_schema_privilege(
    'phanta_app',
    'public',
    'USAGE'
)
""")

schema_usage = cur.fetchone()[0]


# ------------------------------------------------------------
# 7. Sequence privileges
# ------------------------------------------------------------

cur.execute("""
SELECT
    sequence_name,
    has_sequence_privilege(
        'phanta_app',
        'public.' || quote_ident(sequence_name),
        'USAGE'
    ) AS usage,
    has_sequence_privilege(
        'phanta_app',
        'public.' || quote_ident(sequence_name),
        'SELECT'
    ) AS select_priv
FROM information_schema.sequences
WHERE sequence_schema = 'public'
ORDER BY sequence_name
""")

sequence_rows = cur.fetchall()


# ------------------------------------------------------------
# 8. Role security properties
# ------------------------------------------------------------

cur.execute("""
SELECT
    rolname,
    rolsuper,
    rolcreaterole,
    rolcreatedb,
    rolbypassrls,
    rolcanlogin
FROM pg_roles
WHERE rolname = 'phanta_app'
""")

role = cur.fetchone()


# ------------------------------------------------------------
# 9. Output
# ------------------------------------------------------------

print()
print("=" * 80)
print("PHANTA DATABASE / APPLICATION ROLE AUDIT")
print("=" * 80)

print()
print("ROLE SECURITY")
print("-" * 80)

if role:
    print("ROLE:", role[0])
    print("SUPERUSER:", role[1])
    print("CREATE ROLE:", role[2])
    print("CREATE DATABASE:", role[3])
    print("BYPASS RLS:", role[4])
    print("CAN LOGIN:", role[5])
else:
    print("ERROR: phanta_app role does not exist")


print()
print("SCHEMA SECURITY")
print("-" * 80)
print("public USAGE:", schema_usage)


print()
print("ACTUAL DATABASE TABLES")
print("-" * 80)

for schema, table, owner in db_tables:
    rls_enabled, rls_forced = rls.get(table.lower(), (False, False))
    grants = privileges.get(table.lower(), set())

    print(
        f"{table:40} "
        f"OWNER={owner:15} "
        f"RLS={str(rls_enabled):5} "
        f"FORCED={str(rls_forced):5} "
        f"GRANTS={','.join(sorted(grants)) if grants else 'NONE'}"
    )


print()
print("TABLES REFERENCED BY PYTHON CODE")
print("-" * 80)

for table in sorted(code_tables):
    exists = table in db_table_names
    print(
        f"{table:40} "
        f"{'EXISTS' if exists else 'MISSING'}"
    )


print()
print("CODE TABLES MISSING FROM DATABASE")
print("-" * 80)

missing = sorted(code_tables - db_table_names)

if missing:
    for table in missing:
        print(table)
else:
    print("NONE")


print()
print("DATABASE TABLES NOT FOUND IN CODE SQL REFERENCES")
print("-" * 80)

unused = sorted(db_table_names - code_tables)

if unused:
    for table in unused:
        print(table)
else:
    print("NONE")


print()
print("PHANTA_APP PRIVILEGE GAPS")
print("-" * 80)

required = {
    "SELECT",
    "INSERT",
    "UPDATE",
    "DELETE",
}

for table in sorted(db_table_names):
    grants = privileges.get(table, set())
    missing_privs = sorted(required - grants)

    if missing_privs:
        print(
            f"{table:40} "
            f"MISSING={','.join(missing_privs)}"
        )

print()
print("SEQUENCE PRIVILEGES")
print("-" * 80)

for sequence, usage, select_priv in sequence_rows:
    print(
        f"{sequence:45} "
        f"USAGE={str(usage):5} "
        f"SELECT={str(select_priv):5}"
    )


print()
print("RLS TABLES WITHOUT PHANTA_APP FULL DML PRIVILEGES")
print("-" * 80)

for table in sorted(db_table_names):
    rls_enabled, rls_forced = rls.get(table, (False, False))
    grants = privileges.get(table, set())

    if rls_enabled:
        missing_privs = sorted(required - grants)

        if missing_privs:
            print(
                f"{table:40} "
                f"RLS=ON "
                f"MISSING={','.join(missing_privs)}"
            )


print()
print("=" * 80)
print("AUDIT COMPLETE")
print("=" * 80)

cur.close()
conn.close()
