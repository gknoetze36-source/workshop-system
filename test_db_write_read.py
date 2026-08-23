import psycopg2
import os

c = psycopg2.connect(os.environ["DATABASE_URL"])
c.autocommit = False
cur = c.cursor()

try:
    print("=== DB WRITE/READ TEST ===")

    cur.execute("""
        INSERT INTO locations
        (
            name,
            legal_name,
            review_request_enabled,
            active,
            created_at,
            updated_at
        )
        VALUES
        (
            '__DB_TEST__',
            '__DB_TEST__',
            false,
            true,
            NOW(),
            NOW()
        )
        RETURNING id, name, review_request_enabled, active
    """)

    created = cur.fetchone()
    print("WRITE:", created)

    cur.execute("""
        SELECT id, name, review_request_enabled, active
        FROM locations
        WHERE id = %s
    """, (created[0],))

    read_back = cur.fetchone()
    print("READ :", read_back)

    if read_back != created:
        raise RuntimeError("READ/WRITE MISMATCH")

    cur.execute("""
        DELETE FROM locations
        WHERE id = %s
        RETURNING id
    """, (created[0],))

    deleted = cur.fetchone()
    print("DELETE:", deleted)

    c.commit()

    if deleted is None:
        raise RuntimeError("TEST RECORD WAS NOT DELETED")

    print("RESULT: PASS")

except Exception as e:
    c.rollback()
    print("RESULT: FAIL")
    print(type(e).__name__ + ":", e)
    raise

finally:
    cur.close()
    c.close()
