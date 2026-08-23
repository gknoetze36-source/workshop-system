import psycopg2
import os

c = psycopg2.connect(os.environ["DATABASE_URL"])
cur = c.cursor()

cur.execute("""
SELECT
    schemaname,
    tablename,
    tableowner
FROM pg_tables
WHERE schemaname = 'public'
ORDER BY tablename
""")

print("PUBLIC TABLE OWNERS:")
for row in cur.fetchall():
    print(row)

cur.close()
c.close()
