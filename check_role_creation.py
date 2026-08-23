import psycopg2
import os

c = psycopg2.connect(os.environ["DATABASE_URL"])
cur = c.cursor()

cur.execute("""
SELECT
    current_user,
    rolcreaterole,
    rolsuper,
    rolbypassrls
FROM pg_roles
WHERE rolname = current_user
""")

print("CURRENT ROLE CAPABILITIES:")
print(cur.fetchone())

cur.close()
c.close()
