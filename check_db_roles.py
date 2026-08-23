import psycopg2
import os

c = psycopg2.connect(os.environ["DATABASE_URL"])
cur = c.cursor()

cur.execute("""
SELECT rolname, rolsuper, rolbypassrls, rolcanlogin
FROM pg_roles
ORDER BY rolname
""")

print("=== POSTGRES ROLES ===")
for row in cur.fetchall():
    print(row)

cur.close()
c.close()
