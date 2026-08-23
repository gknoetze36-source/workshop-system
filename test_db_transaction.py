import os
import psycopg2
from datetime import datetime, timezone

TEST_NAME = "__DB_TRANSACTION_TEST__"

print("=== DB TRANSACTION / ROLLBACK TEST ===")

conn = psycopg2.connect(os.environ["DATABASE_URL"])

try:
    cur = conn.cursor()

    now = datetime.now(timezone.utc)

    cur.execute("""
        INSERT INTO locations
            (name, review_request_enabled, active, created_at, updated_at)
        VALUES
            (%s, %s, %s, %s, %s)
        RETURNING id
    """, (
        TEST_NAME,
        False,
        True,
        now,
        now
    ))

    location_id = cur.fetchone()[0]

    print("WRITE: location created:", location_id)

    # Intentionally roll back
    conn.rollback()

    print("ROLLBACK: transaction rolled back")

    # Verify the row was NOT persisted
    cur.execute(
        "SELECT id FROM locations WHERE id = %s",
        (location_id,)
    )

    result = cur.fetchone()

    if result is None:
        print("VERIFY: test row does not exist")
        print("RESULT: PASS")
    else:
        print("VERIFY: test row still exists")
        print("RESULT: FAIL")

    cur.close()

finally:
    conn.close()
