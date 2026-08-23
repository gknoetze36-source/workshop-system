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
WHERE table_schema = 'public'
ORDER BY grantee, table_name, privilege_type
""")

print("EXPLICIT TABLE GRANTS:")

rows = cur.fetchall()

if not rows:
    print("NO EXPLICIT TABLE GRANTS FOUND")
else:
    for row in rows:
        print(row)

cur.close()
c.close()
