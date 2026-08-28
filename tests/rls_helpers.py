"""RLS-aware seeding helpers for the behavioural test suites.

WHY THIS EXISTS
---------------
The security and onboarding test fixtures seed data through the raw
execute_db/query_db layer, outside any Flask request. On SQLite that works,
because SQLite has no row-level security. On PostgreSQL under the properly
restricted `phanta_app` role it does NOT: RLS is FORCED on customers,
bookings, audit_logs and friends, `app.location_id` is unset outside a request,
and every seeding INSERT fails with

    new row violates row-level security policy for table "customers"

That meant the behavioural suites could only ever run against SQLite -- the
backend that ignores the very isolation those suites exist to prove. Tests that
cannot run against the production database engine are not evidence about
production.

WHAT THESE HELPERS DO
---------------------
`platform_scope()` sets app.platform_admin for the duration of a block, which
is how the platform read/write policies allow tenant-agnostic rows (owners,
locations, platform tables) to be created.

`location_scope(location_id)` is a thin re-export of the production helper
`database.query.raw_location_scope`, so tenant-scoped seeding goes through the
exact same mechanism the cron jobs use rather than a test-only backdoor.

Both are no-ops on SQLite, so the same fixtures work on either backend.
"""
from contextlib import contextmanager

from database.connection import _LOCAL, get_connection
from database.query import raw_location_scope

__all__ = ["platform_scope", "location_scope", "is_postgres"]


def is_postgres() -> bool:
    connection, backend = get_connection()
    try:
        return backend == "postgres"
    finally:
        if not getattr(_LOCAL, "connection", None):
            connection.close()


@contextmanager
def platform_scope():
    """Seed rows that belong to no single tenant (owners, locations, platform tables).

    Mirrors the platform-admin context the application itself uses for its
    platform dashboards, rather than granting the test role BYPASSRLS -- the
    point is to exercise the same policies production runs under.
    """
    if getattr(_LOCAL, "connection", None):
        connection, backend = _LOCAL.connection
        if backend == "postgres":
            with connection.cursor() as cursor:
                cursor.execute("SELECT set_config('app.platform_admin', '1', true)")
        try:
            yield
        finally:
            if backend == "postgres":
                with connection.cursor() as cursor:
                    cursor.execute("SELECT set_config('app.platform_admin', '', true)")
        return

    connection, backend = get_connection()
    if backend == "postgres":
        with connection.cursor() as cursor:
            cursor.execute("SELECT set_config('app.platform_admin', '1', true)")
    _LOCAL.connection = (connection, backend)
    _LOCAL.in_transaction = True
    try:
        yield
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        _LOCAL.connection = None
        _LOCAL.in_transaction = False
        connection.close()


@contextmanager
def location_scope(location_id):
    """Seed tenant-scoped rows using the production raw-layer RLS helper."""
    with raw_location_scope(int(location_id)):
        yield
