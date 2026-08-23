import psycopg2
import os

c = psycopg2.connect(os.environ["DATABASE_URL"])
cur = c.cursor()

cur.execute("""
SELECT
    current_database(),
    current_user,
    rolsuper,
    rolcreaterole,
    rolbypassrls,
    rolcanlogin
FROM pg_roles
WHERE rolname = current_user
""")

print("CURRENT DATABASE / ROLE:")
print(cur.fetchone())

cur.close()
c.close()
