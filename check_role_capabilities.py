import psycopg2
import os

c = psycopg2.connect(os.environ["DATABASE_URL"])
cur = c.cursor()

cur.execute("""
SELECT current_database(), current_user, version()
""")

print("DATABASE / ROLE:")
print(cur.fetchone())

cur.execute("""
SELECT has_schema_privilege(current_user, 'public', 'CREATE')
""")

print("CAN CREATE IN PUBLIC:")
print(cur.fetchone()[0])

cur.close()
c.close()
