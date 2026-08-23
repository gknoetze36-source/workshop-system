import os
import psycopg2

c = psycopg2.connect(os.environ["DATABASE_URL"])
cur = c.cursor()

cur.execute("""
SELECT
    t.table_name,
    pg_get_userbyid(c.relowner) AS owner,
    c.relrowsecurity AS rls_enabled,
    c.relforcerowsecurity AS rls_forced,
    has_table_privilege('phanta_app', t.table_schema || '.' || t.table_name, 'SELECT') AS can_select,
    has_table_privilege('phanta_app', t.table_schema || '.' || t.table_name, 'INSERT') AS can_insert,
    has_table_privilege('phanta_app', t.table_schema || '.' || t.table_name, 'UPDATE') AS can_update,
    has_table_privilege('phanta_app', t.table_schema || '.' || t.table_name, 'DELETE') AS can_delete
FROM information_schema.tables t
JOIN pg_class c
    ON c.relname = t.table_name
JOIN pg_namespace n
    ON n.oid = c.relnamespace
   AND n.nspname = t.table_schema
WHERE t.table_schema = 'public'
  AND t.table_type = 'BASE TABLE'
ORDER BY t.table_name
""")

print("=== PHANTA DATABASE SECURITY MATRIX ===")
for row in cur.fetchall():
    print(row)

cur.close()
c.close()
