import psycopg2
import os

c = psycopg2.connect(os.environ["DATABASE_URL"])
cur = c.cursor()

cur.execute("""
SELECT
    current_user,
    session_user,
    rolsuper,
    rolbypassrls
FROM pg_roles
WHERE rolname = current_user
""")

print("=== CURRENT ROLE RLS STATUS ===")
for row in cur.fetchall():
    print(row)

cur.close()
c.close()
