import os
import pytest
from sqlalchemy import create_engine, text

POSTGRES_URL = os.getenv("PHANTA_TEST_POSTGRES_URL") or os.getenv("DATABASE_URL", "")
pytestmark = pytest.mark.skipif(not POSTGRES_URL.startswith(("postgresql://", "postgresql+")), reason="PostgreSQL test database not configured")

def test_postgres_has_rls_and_fail_closed():
    engine = create_engine(POSTGRES_URL)
    with engine.begin() as conn:
        rows = conn.execute(text("""
            SELECT relname, relrowsecurity, relforcerowsecurity
            FROM pg_class
            WHERE relname IN ('customers','vehicles','bookings','conversations','quotes','audit_logs')
        """)).mappings().all()
    assert rows
    assert all(r["relrowsecurity"] and r["relforcerowsecurity"] for r in rows)

def test_postgres_booking_exclusion_constraints_exist():
    engine = create_engine(POSTGRES_URL)
    with engine.begin() as conn:
        names = {r[0] for r in conn.execute(text("""
            SELECT conname FROM pg_constraint
            WHERE conname IN ('bookings_no_bay_overlap','bookings_no_technician_overlap')
        """))}
    assert {"bookings_no_bay_overlap", "bookings_no_technician_overlap"} <= names

def test_postgres_approval_is_append_only():
    engine = create_engine(POSTGRES_URL)
    with engine.begin() as conn:
        names = {r[0] for r in conn.execute(text("""
            SELECT tgname FROM pg_trigger
            WHERE tgname = 'approvals_append_only'
        """))}
    assert "approvals_append_only" in names
