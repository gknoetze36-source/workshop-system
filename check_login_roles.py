import psycopg2
import os

c = psycopg2.connect(os.environ["DATABASE_URL"])
cur = c.cursor()

cur.execute("""
SELECT current_database(), current_user, current_setting('role')
""")

print("CURRENT DATABASE / ROLE:")
print(cur.fetchone())

cur.execute("""
SELECT rolname, rolsuper, rolbypassrls, rolcanlogin
FROM pg_roles
WHERE rolcanlogin = true
ORDER BY rolname
""")

print("LOGIN ROLES:")
for row in cur.fetchall():
    print(row)

cur.close()
c.close()
