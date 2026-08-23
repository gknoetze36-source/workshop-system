import psycopg2
import os

c = psycopg2.connect(os.environ["DATABASE_URL"])
cur = c.cursor()

print("=== DATABASE ===")
cur.execute("SELECT current_database(), current_schema(), version()")
for row in cur.fetchall():
    print(row)

print("\n=== ALEMBIC ===")
cur.execute("""
SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_name = 'alembic_version'
ORDER BY table_schema
""")
for row in cur.fetchall():
    print(row)

print("\n=== ALEMBIC VERSION ===")
try:
    cur.execute("SELECT version_num FROM alembic_version")
    for row in cur.fetchall():
        print(row)
except Exception as e:
    print("ERROR:", e)
    c.rollback()

print("\n=== IMPORTANT TABLES ===")
cur.execute("""
SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_name IN (
    'locations',
    'customers',
    'vehicles',
    'bookings',
    'conversations',
    'quotes',
    'audit_logs',
    'approvals'
)
ORDER BY table_schema, table_name
""")
for row in cur.fetchall():
    print(row)

print("\n=== ALL PUBLIC TABLES ===")
cur.execute("""
SELECT tablename
FROM pg_tables
WHERE schemaname = 'public'
ORDER BY tablename
""")
for row in cur.fetchall():
    print(row)

cur.close()
c.close()
