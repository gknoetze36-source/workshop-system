import psycopg2
import os

c = psycopg2.connect(os.environ["DATABASE_URL"])
cur = c.cursor()

cur.execute("""
SELECT relname, relrowsecurity, relforcerowsecurity
FROM pg_class
WHERE relname IN (
    'customers',
    'vehicles',
    'bookings',
    'conversations',
    'quotes',
    'audit_logs'
)
ORDER BY relname
""")

print("RLS:")
for row in cur.fetchall():
    print(row)

cur.execute("""
SELECT conname
FROM pg_constraint
WHERE conname IN (
    'bookings_no_bay_overlap',
    'bookings_no_technician_overlap'
)
ORDER BY conname
""")

print("\nCONSTRAINTS:")
for row in cur.fetchall():
    print(row)

cur.execute("""
SELECT tgname
FROM pg_trigger
WHERE tgname = 'approvals_append_only'
""")

print("\nTRIGGER:")
for row in cur.fetchall():
    print(row)

cur.close()
c.close()
