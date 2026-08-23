import psycopg2
import os

print("=== APP DATABASE ROLE / PERMISSIONS TEST ===")

c = psycopg2.connect(os.environ["DATABASE_URL"])
cur = c.cursor()

cur.execute("""
SELECT
    current_user,
    rolsuper,
    rolbypassrls,
    rolcanlogin
FROM pg_roles
WHERE rolname = current_user
""")

print("CURRENT ROLE:")
print(cur.fetchone())

cur.execute("""
SELECT
    has_database_privilege(current_user, current_database(), 'CONNECT'),
    has_schema_privilege(current_user, 'public', 'USAGE')
""")

print("DATABASE / SCHEMA PERMISSIONS:")
print(cur.fetchone())

cur.execute("""
SELECT
    table_name,
    has_table_privilege(current_user, 'public.' || table_name, 'SELECT'),
    has_table_privilege(current_user, 'public.' || table_name, 'INSERT'),
    has_table_privilege(current_user, 'public.' || table_name, 'UPDATE'),
    has_table_privilege(current_user, 'public.' || table_name, 'DELETE')
FROM information_schema.tables
WHERE table_schema = 'public'
  AND table_name IN (
      'locations',
      'customers',
      'vehicles',
      'bookings',
      'conversations',
      'quotes',
      'audit_logs'
  )
ORDER BY table_name
""")

print("TABLE PERMISSIONS:")
for row in cur.fetchall():
    print(row)

cur.close()
c.close()
