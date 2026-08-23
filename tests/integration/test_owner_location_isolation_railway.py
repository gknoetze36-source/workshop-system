"""
PHANTA production Owner -> Location isolation test.

Run inside the Railway web container after dependencies are installed:

    python tests/integration/test_owner_location_isolation_railway.py

This test intentionally requires a PostgreSQL DATABASE_URL. It creates two
temporary owners/locations and temporary core records, verifies PostgreSQL RLS,
then exercises the real Flask application with Owner A and Owner B sessions.

It never prints DATABASE_URL or credentials.
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta, timezone

if not os.getenv("DATABASE_URL", "").startswith(("postgresql://", "postgresql+")):
    raise SystemExit("RED: DATABASE_URL is not a PostgreSQL URL; run this inside Railway.")

import psycopg2
from flask import session as flask_session
from sqlalchemy import text

from phanta_app import app
from database import SessionLocal


def check(ok: bool, message: str) -> None:
    print(("PASS: " if ok else "FAIL: ") + message)
    if not ok:
        raise AssertionError(message)


def main() -> None:
    now = datetime.now(timezone.utc)
    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    conn.autocommit = False
    cur = conn.cursor()
    owner_ids = []
    location_ids = []
    ids = {}

    try:
        # ------------------------------------------------------------
        # 1. Create two independent Owner -> Location contexts.
        # ------------------------------------------------------------
        for suffix in ("A", "B"):
            cur.execute(
                """
                INSERT INTO owners (name, email, active, created_at, updated_at)
                VALUES (%s, %s, TRUE, %s, %s)
                RETURNING id
                """,
                (f"__ISOLATION_OWNER_{suffix}__", f"isolation-{suffix.lower()}@example.test", now, now),
            )
            owner_id = cur.fetchone()[0]
            owner_ids.append(owner_id)

            cur.execute(
                """
                INSERT INTO locations
                    (owner_id, name, industry, active, created_at, updated_at)
                VALUES (%s, %s, 'workshop', TRUE, %s, %s)
                RETURNING id
                """,
                (owner_id, f"__ISOLATION_LOCATION_{suffix}__", now, now),
            )
            location_ids.append(cur.fetchone()[0])

        loc_a, loc_b = location_ids

        # The unique(owner_id) constraint is the database proof of 1 owner = 1 location.
        cur.execute(
            "SELECT COUNT(*) FROM locations WHERE owner_id = ANY(%s)",
            (owner_ids,),
        )
        check(cur.fetchone()[0] == 2, "two owners each have exactly one location")

        # ------------------------------------------------------------
        # 2. Create Location A and Location B core records.
        # ------------------------------------------------------------
        for loc, suffix, phone in (
            (loc_a, "A", "+27000000101"),
            (loc_b, "B", "+27000000102"),
        ):
            cur.execute(
                """
                SELECT set_config('app.location_id', %s, true)
                """,
                (str(loc),),
            )
            cur.execute(
                """
                INSERT INTO customers
                    (location_id, first_name, last_name, whatsapp_number, created_at, updated_at)
                VALUES (%s, %s, 'Isolation', %s, %s, %s)
                RETURNING id
                """,
                (loc, f"Owner{suffix}", phone, now, now),
            )
            customer_id = cur.fetchone()[0]

            cur.execute(
                """
                INSERT INTO vehicles
                    (location_id, customer_id, make, model, year)
                VALUES (%s, %s, 'Test', %s, 2026)
                RETURNING id
                """,
                (loc, customer_id, f"Vehicle{suffix}"),
            )
            vehicle_id = cur.fetchone()[0]

            cur.execute(
                """
                INSERT INTO bookings
                    (location_id, customer_id, vehicle_id, start_time, end_time, service_type)
                VALUES (%s, %s, %s, %s, %s, 'Service')
                RETURNING id
                """,
                (loc, customer_id, vehicle_id, now + timedelta(days=1), now + timedelta(days=1, hours=1)),
            )
            booking_id = cur.fetchone()[0]

            ids[suffix] = {
                "customer": customer_id,
                "vehicle": vehicle_id,
                "booking": booking_id,
            }

        conn.commit()

        # ------------------------------------------------------------
        # 3. Verify PostgreSQL RLS is actually enabled and forced.
        # ------------------------------------------------------------
        cur.execute(
            """
            SELECT relname, relrowsecurity, relforcerowsecurity
            FROM pg_class
            WHERE relname = ANY(%s)
            """,
            ([
                "customers", "vehicles", "bookings", "booking_inquiries",
                "notes", "conversations", "messages", "services",
                "automation_rules", "scheduled_jobs", "automation_logs",
                "failed_jobs", "meta_business_connections",
                "meta_webhook_events", "payments", "subscriptions",
                "invoices", "paystack_webhook_events",
            ],),
        )
        rls_rows = {r[0]: (r[1], r[2]) for r in cur.fetchall()}
        for table in ("customers", "vehicles", "bookings", "booking_inquiries", "notes"):
            if table in rls_rows:
                check(
                    rls_rows[table] == (True, True),
                    f"{table} has FORCE ROW LEVEL SECURITY",
                )

        # ------------------------------------------------------------
        # 4. Direct PostgreSQL cross-location reads/writes.
        # ------------------------------------------------------------
        cur.execute("SELECT set_config('app.location_id', %s, true)", (str(loc_a),))

        for table, key in (
            ("customers", "customer"),
            ("vehicles", "vehicle"),
            ("bookings", "booking"),
        ):
            cur.execute(
                f"SELECT id FROM {table} WHERE id = %s",
                (ids["B"][key],),
            )
            check(
                cur.fetchone() is None,
                f"Location A cannot READ Location B {table}",
            )

            cur.execute(
                f"UPDATE {table} SET id = id WHERE id = %s",
                (ids["B"][key],),
            )
            check(
                cur.rowcount == 0,
                f"Location A cannot UPDATE Location B {table}",
            )

            cur.execute(
                f"DELETE FROM {table} WHERE id = %s",
                (ids["B"][key],),
            )
            check(
                cur.rowcount == 0,
                f"Location A cannot DELETE Location B {table}",
            )

        # Cross-location INSERT must be rejected by WITH CHECK.
        cur.execute(
            """
            INSERT INTO customers
                (location_id, first_name, last_name, whatsapp_number, created_at, updated_at)
            VALUES (%s, '__CROSS__', 'INSERT', '+27000000999', %s, %s)
            """,
            (loc_b, now, now),
        )
        raise AssertionError("FAIL: Location A was able to INSERT into Location B")

    except psycopg2.Error as exc:
        # The expected cross-location INSERT is normally rejected by RLS.
        if "row-level security" in str(exc).lower():
            conn.rollback()
            print("PASS: Location A cross-location INSERT rejected by PostgreSQL RLS")
        else:
            conn.rollback()
            raise

    try:
        # ------------------------------------------------------------
        # 5. Real Flask application read isolation.
        # ------------------------------------------------------------
        client = app.test_client()

        with client.session_transaction() as sess:
            sess["user"] = {
                "id": owner_ids[0],
                "username": "isolation-a@example.test",
                "email": "isolation-a@example.test",
                "role": "owner",
                "owner_id": owner_ids[0],
                "location_id": loc_a,
            }

        for url, label in (
            (f"/customers/{ids['B']['customer']}", "customer"),
            (f"/vehicles/{ids['B']['vehicle']}", "vehicle"),
        ):
            response = client.get(url)
            check(
                response.status_code in (403, 404),
                f"Owner A cannot access Location B {label} through {url} "
                f"(HTTP {response.status_code})",
            )

        own = client.get(f"/customers/{ids['A']['customer']}")
        check(
            own.status_code == 200,
            f"Owner A can access own customer (HTTP {own.status_code})",
        )

        own_vehicle = client.get(f"/vehicles/{ids['A']['vehicle']}")
        check(
            own_vehicle.status_code == 200,
            f"Owner A can access own vehicle (HTTP {own_vehicle.status_code})",
        )

        # Session tampering: changing only the location_id must not make the
        # authenticated owner a different owner.
        with client.session_transaction() as sess:
            sess["user"]["location_id"] = loc_b

        tampered = client.get(f"/customers/{ids['B']['customer']}")
        # This proves the route follows session location, but the session itself
        # is cryptographically signed in a real browser. A forged session cookie
        # is outside this test's threat model; direct DB/RLS still protects data.
        check(
            tampered.status_code in (403, 404),
            "tampered Owner A session cannot use Location B data",
        )

        print("RESULT: PASS — Owner/Location isolation checks completed")

    finally:
        # ------------------------------------------------------------
        # 6. Cleanup using each owning Location context.
        # ------------------------------------------------------------
        for loc, suffix in ((loc_a, "A"), (loc_b, "B")):
            cur.execute("SELECT set_config('app.location_id', %s, true)", (str(loc),))
            cur.execute("DELETE FROM bookings WHERE id = %s", (ids[suffix]["booking"],))
            cur.execute("DELETE FROM vehicles WHERE id = %s", (ids[suffix]["vehicle"],))
            cur.execute("DELETE FROM customers WHERE id = %s", (ids[suffix]["customer"],))
            conn.commit()

        # Locations/owners are not location-RLS protected.
        cur.execute("DELETE FROM locations WHERE id = ANY(%s)", (location_ids,))
        cur.execute("DELETE FROM owners WHERE id = ANY(%s)", (owner_ids,))
        conn.commit()
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
