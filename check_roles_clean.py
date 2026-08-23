import psycopg2
import os

c = psycopg2.connect(os.environ["DATABASE_URL"])
cur = c.cursor()

cur.execute("""
SELECT
    rolname,
    rolcanlogin,
    rolsuper,
    rolcreaterole,
    rolcreatedb,
    rolreplication,
    rolbypassrls
FROM pg_roles
WHERE rolname NOT LIKE 'pg_%'
ORDER BY rolname
""")

print("NON-SYSTEM DATABASE ROLES:")
for row in cur.fetchall():
    print(row)

cur.close()
c.close()
