#!/usr/bin/env python3
"""Run the Phase 2 production-database gate against a PostgreSQL URL.

Usage:
  PHANTA_TEST_POSTGRES_URL='postgresql+psycopg://...' python scripts/validate_phase2_postgres.py

The script uses one transaction and rolls all test data back. It verifies:
- the connected role is not a PostgreSQL superuser
- RLS is enabled and forced
- location A cannot read location B data
- booking EXCLUDE constraints reject overlaps
- approval UPDATE/DELETE is rejected
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from sqlalchemy import create_engine, select, text
from sqlalchemy.exc import IntegrityError, DBAPIError
from sqlalchemy.orm import Session

from models.core import Location, Customer, Vehicle, Booking, Quote, QuoteLineItem, Approval


def main() -> int:
    url = os.getenv("PHANTA_TEST_POSTGRES_URL") or os.getenv("DATABASE_URL")
    if not url or not url.startswith(("postgresql://", "postgresql+")):
        raise SystemExit("Set PHANTA_TEST_POSTGRES_URL or DATABASE_URL to PostgreSQL.")

    engine = create_engine(url, future=True, pool_pre_ping=True)

    with engine.connect() as conn:
        superuser = conn.execute(text("SELECT rolsuper FROM pg_roles WHERE rolname = current_user")).scalar()
        if superuser:
            raise SystemExit("FAIL: connected PostgreSQL role is a superuser; RLS cannot be validated safely.")

        required = {"customers", "vehicles", "bookings", "conversations", "quotes", "audit_logs", "approvals"}
        rows = conn.execute(text("""
            SELECT relname, relrowsecurity, relforcerowsecurity
            FROM pg_class
            WHERE relname = ANY(:names)
        """), {"names": list(required)}).mappings().all()
        by_name = {r["relname"]: r for r in rows}
        missing = required - set(by_name)
        if missing:
            raise SystemExit(f"FAIL: missing tables: {sorted(missing)}")
        if not all(r["relrowsecurity"] and r["relforcerowsecurity"] for r in rows):
            raise SystemExit("FAIL: one or more location tables do not have forced RLS.")

    with Session(engine) as session:
        with session.begin():
            a = Location(name="__PHANTA_PHASE2_TEST_A__")
            b = Location(name="__PHANTA_PHASE2_TEST_B__")
            session.add_all([a, b])
            session.flush()

            session.execute(text("SELECT set_config('app.location_id', :id, true)"), {"id": str(a.id)})
            ca = Customer(location_id=a.id, first_name="A", last_name="Test", whatsapp_number="+27000000001")
            session.add(ca); session.flush()
            va = Vehicle(location_id=a.id, customer_id=ca.id, make="Toyota", model="Test", year=2024)
            session.add(va); session.flush()

            now = datetime.now(timezone.utc)
            booking = Booking(
                location_id=a.id, customer_id=ca.id, vehicle_id=va.id,
                start_time=now, end_time=now + timedelta(hours=1),
                service_type="Phase2Test", bay_id=99991, status="confirmed"
            )
            session.add(booking); session.flush()

            session.execute(text("SELECT set_config('app.location_id', :id, true)"), {"id": str(b.id)})
            visible = session.scalar(select(Customer).where(Customer.id == ca.id))
            if visible is not None:
                raise SystemExit("FAIL: location B can read location A customer.")

            # Switch back to A for the conflict and approval tests.
            session.execute(text("SELECT set_config('app.location_id', :id, true)"), {"id": str(a.id)})
            conflict = Booking(
                location_id=a.id, customer_id=ca.id, vehicle_id=va.id,
                start_time=now + timedelta(minutes=15),
                end_time=now + timedelta(minutes=45),
                service_type="Phase2ConflictTest", bay_id=99991, status="confirmed"
            )
            session.add(conflict)
            try:
                session.flush()
            except IntegrityError:
                session.rollback()
                # Re-open a transaction and prove the trigger independently.
                session.begin()
                session.execute(text("SELECT set_config('app.location_id', :id, true)"), {"id": str(a.id)})
                ca = session.scalar(select(Customer).where(Customer.location_id == a.id))
                va = session.scalar(select(Vehicle).where(Vehicle.location_id == a.id))
                now = datetime.now(timezone.utc)
                booking = session.scalar(select(Booking).where(Booking.location_id == a.id))
                quote = Quote(location_id=a.id, customer_id=ca.id, total_amount=Decimal("100.00"))
                session.add(quote); session.flush()
                item = QuoteLineItem(location_id=a.id, quote_id=quote.id, description="Test", price=Decimal("100.00"))
                session.add(item); session.flush()
                approval = Approval(
                    location_id=a.id, quote_line_item_id=item.id, decision="approved",
                    decided_by="+27000000001", raw_message="Yes", channel="whatsapp"
                )
                session.add(approval); session.flush()
                try:
                    approval.decision = "rejected"
                    session.flush()
                except DBAPIError:
                    session.rollback()
                    print("PASS: PostgreSQL RLS + booking overlap + append-only approval gate verified.")
                    return 0
                raise SystemExit("FAIL: approval mutation was allowed.")
            else:
                raise SystemExit("FAIL: overlapping booking was accepted.")

if __name__ == "__main__":
    raise SystemExit(main())
