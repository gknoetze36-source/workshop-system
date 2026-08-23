import os
import psycopg2

c = psycopg2.connect(os.environ["DATABASE_URL"])
cur = c.cursor()

cur.execute("""
SELECT
    schemaname,
    tablename,
    policyname,
    permissive,
    roles,
    cmd,
    qual,
    with_check
FROM pg_policies
WHERE schemaname = 'public'
  AND tablename IN (
      'audit_logs',
      'bookings',
      'conversations',
      'customers',
      'quotes',
      'vehicles'
  )
ORDER BY tablename, policyname
""")

print("=== RLS POLICIES ===")

for row in cur.fetchall():
    print("\\nTABLE:", row[1])
    print("POLICY:", row[2])
    print("COMMAND:", row[5])
    print("USING:", row[6])
    print("WITH CHECK:", row[7])

cur.close()
c.close()
