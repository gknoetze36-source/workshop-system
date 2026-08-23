import psycopg2
import os

c = psycopg2.connect(os.environ["DATABASE_URL"])
cur = c.cursor()

cur.execute("""
SELECT
    current_database(),
    current_user,
    current_setting('server_version')
""")

print("DATABASE:")
print(cur.fetchone())

cur.execute("""
SELECT
    rolname,
    rolsuper,
    rolcreaterole,
    rolcreatedb,
    rolcanlogin,
    rolreplication,
    rolbypassrls
FROM pg_roles
ORDER BY rolname
""")

print("\nALL ROLES:")
for row in cur.fetchall():
    print(row)

cur.execute("""
SELECT
    has_database_privilege(current_user, current_database(), 'CREATE'),
    has_schema_privilege(current_user, 'public', 'CREATE'),
    has_schema_privilege(current_user, 'public', 'USAGE')
""")

print("\nCURRENT ROLE PRIVILEGES:")
print(cur.fetchone())

cur.close()
c.close()
