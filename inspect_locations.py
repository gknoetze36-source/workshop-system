import os
import psycopg2

c = psycopg2.connect(os.environ["DATABASE_URL"])
cur = c.cursor()

cur.execute("""
SELECT
    column_name,
    data_type,
    is_nullable,
    column_default
FROM information_schema.columns
WHERE table_schema = 'public'
  AND table_name = 'locations'
ORDER BY ordinal_position
""")

print("=== LOCATIONS TABLE SCHEMA ===")
for row in cur.fetchall():
    print(row)

cur.close()
c.close()
