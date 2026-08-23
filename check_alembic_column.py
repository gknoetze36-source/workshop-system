import psycopg2
import os

c = psycopg2.connect(os.environ["DATABASE_URL"])
cur = c.cursor()

cur.execute("""
SELECT column_name, data_type, character_maximum_length
FROM information_schema.columns
WHERE table_name = 'alembic_version'
""")

print("ALEMBIC VERSION COLUMN:")
for row in cur.fetchall():
    print(row)

cur.close()
c.close()
