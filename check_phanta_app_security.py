import psycopg2
import os

c = psycopg2.connect(os.environ["DATABASE_URL"])
cur = c.cursor()

cur.execute("""
SELECT
    r.rolname,
    r.rolsuper,
    r.rolcreaterole,
    r.rolbypassrls,
    r.rolcanlogin,
    has_schema_privilege(r.rolname, 'public', 'USAGE'),
    (
        SELECT count(*)
        FROM information_schema.role_table_grants g
        WHERE g.grantee = r.rolname
    )
FROM pg_roles r
WHERE r.rolname = 'phanta_app'
""")

print("PHANTA_APP SECURITY STATUS:")
print(cur.fetchone())

cur.close()
c.close()
