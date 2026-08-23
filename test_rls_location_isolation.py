import os
import psycopg2
from datetime import datetime, timezone

print("=== RLS LOCATION ISOLATION TEST ===")

conn = psycopg2.connect(os.environ["DATABASE_URL"])

try:
    cur = conn.cursor()
    now = datetime.now(timezone.utc)

    # Disable automatic transaction leakage between test sections
    conn.autocommit = False

    # Create two owners and their one-to-one locations.
    owner_ids = []
    location_ids = []
    for suffix in ("A", "B"):
        cur.execute("""
            INSERT INTO owners (name, email, active, created_at, updated_at)
            VALUES (%s, %s, TRUE, %s, %s)
            RETURNING id
        """, (f"__RLS_OWNER_{suffix}__", f"rls-{suffix.lower()}@example.test", now, now))
        owner_id = cur.fetchone()[0]
        owner_ids.append(owner_id)
        cur.execute("""
            INSERT INTO locations
                (owner_id, name, industry, review_request_enabled, active, created_at, updated_at)
            VALUES
                (%s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (owner_id, f"__RLS_LOCATION_{suffix}__", "workshop", False, True, now, now))
        location_ids.append(cur.fetchone()[0])

    location_a, location_b = location_ids

    conn.commit()

    print("LOCATION A:", location_a)
    print("LOCATION B:", location_b)

    # ---------------------------------------------------------
    # Create a customer belonging to Location A.
    # The session location is explicitly set to Location A.
    # ---------------------------------------------------------
    cur.execute("SELECT set_config('app.location_id', %s, false)",
                (str(location_a),))

    cur.execute("""
        INSERT INTO customers
            (location_id, first_name, last_name, whatsapp_number,
             created_at, updated_at)
        VALUES
            (%s, %s, %s, %s, %s, %s)
        RETURNING id
    """, (
        location_a,
        "__RLS__",
        "__TEST__",
        "+27000000001",
        now,
        now
    ))

    customer_id = cur.fetchone()[0]
    conn.commit()

    print("CUSTOMER CREATED:", customer_id)

    # ---------------------------------------------------------
    # Switch session to Location B.
    # Location B must NOT be able to see Location A's customer.
    # ---------------------------------------------------------
    cur.execute("SELECT set_config('app.location_id', %s, false)",
                (str(location_b),))

    cur.execute("""
        SELECT id, location_id
        FROM customers
        WHERE id = %s
    """, (customer_id,))

    result = cur.fetchone()

    if result is None:
        print("ISOLATION: Location B cannot see Location A customer")
        print("RESULT: PASS")
    else:
        print("ISOLATION: FAIL")
        print("Location B can see:", result)
        print("RESULT: FAIL")

    # Cleanup as the owning location.
    cur.execute("SELECT set_config('app.location_id', %s, false)",
                (str(location_a),))

    cur.execute(
        "DELETE FROM customers WHERE id = %s",
        (customer_id,)
    )

    conn.commit()

    # Locations themselves are not RLS protected, so remove both.
    cur.execute("DELETE FROM locations WHERE id IN (%s, %s)",
                (location_a, location_b))
    cur.execute("DELETE FROM owners WHERE id IN (%s, %s)",
                (owner_ids[0], owner_ids[1]))
    conn.commit()

    print("CLEANUP: complete")

    cur.close()

finally:
    conn.close()
