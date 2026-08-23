import os
import psycopg2
from datetime import datetime, timezone

print("=== DB FOREIGN KEY INTEGRITY TEST ===")

conn = psycopg2.connect(os.environ["DATABASE_URL"])

try:
    cur = conn.cursor()
    now = datetime.now(timezone.utc)

    try:
        cur.execute("""
            INSERT INTO customers
                (location_id, first_name, last_name, whatsapp_number,
                 created_at, updated_at)
            VALUES
                (%s, %s, %s, %s, %s, %s)
        """, (
            999999999,
            "__FK_TEST__",
            "__FK_TEST__",
            "+27000000000",
            now,
            now
        ))

        conn.commit()

        print("RESULT: FAIL")
        print("Foreign-key constraint did NOT prevent orphan customer.")

    except psycopg2.errors.ForeignKeyViolation:
        conn.rollback()

        print("FOREIGN KEY: correctly rejected orphan record")
        print("RESULT: PASS")

    except Exception as e:
        conn.rollback()

        print("RESULT: INCONCLUSIVE")
        print(type(e).__name__ + ":", e)

    finally:
        cur.close()

finally:
    conn.close()
