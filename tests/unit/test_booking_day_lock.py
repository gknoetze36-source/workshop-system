"""Two customers booking the last place on a day must take turns (Postgres advisory lock)."""
from datetime import datetime
from unittest.mock import MagicMock

from ai.booking.availability import BookingAvailabilityService


def _service(dialect):
    session = MagicMock()
    session.get_bind.return_value.dialect.name = dialect
    return BookingAvailabilityService(session, schedule=MagicMock()), session


def test_postgres_locks_the_location_day_before_counting():
    service, session = _service("postgresql")
    service._lock_day(7, datetime(2026, 10, 20, 8, 0))
    sql, params = session.execute.call_args.args
    assert "pg_advisory_xact_lock" in str(sql)
    assert params == {"loc": 7, "day": datetime(2026, 10, 20).date().toordinal()}


def test_sqlite_skips_the_lock():
    service, session = _service("sqlite")
    service._lock_day(7, datetime(2026, 10, 20, 8, 0))
    session.execute.assert_not_called()
