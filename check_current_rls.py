import psycopg2
import os

c = psycopg2.connect(os.environ["DATABASE_URL"])
cur = c.cursor()

cur.execute("""
SELECT
    current_user,
    current_setting('app.location_id', true),
    current_setting('app.platform_admin', true)
""")

print("CURRENT USER / RLS SETTINGS:")
print(cur.fetchone())

cur.close()
c.close()
