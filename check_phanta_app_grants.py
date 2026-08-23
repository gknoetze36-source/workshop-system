import psycopg2
import os

c = psycopg2.connect(os.environ["DATABASE_URL"])
cur = c.cursor()

cur.execute("""
SELECT
    grantee,
    table_schema,
    table_name,
    privilege_type
FROM information_schema.role_table_grants
WHERE grantee = 'phanta_app'
ORDER BY table_schema, table_name, privilege_type
""")

print("EXISTING PHANTA_APP TABLE GRANTS:")

rows = cur.fetchall()

if not rows:
    print("NONE")
else:
    for row in rows:
        print(row)

cur.execute("""
SELECT
    grantee,
    privilege_type
FROM information_schema.role_usage_grants
WHERE grantee = 'phanta_app'
ORDER BY privilege_type
""")

print("\nSCHEMA USAGE GRANTS:")
rows = cur.fetchall()

if not rows:
    print("NONE")
else:
    for row in rows:
        print(row)

cur.close()
c.close()
