from contextlib import contextmanager

from .connection import _LOCAL, get_connection

def _adapt_query(query, backend):
    if backend == "sqlite":
        return query.replace("%s", "?")
    return query


def _db_bool(value, backend):
    return bool(value) if backend == "postgres" else int(bool(value))


def _get_cursor(connection, backend):
    if backend == "postgres":
        import psycopg2.extras

        return connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    return connection.cursor()


def _run(connection, backend, query, args=(), one=False):
    cursor = _get_cursor(connection, backend)
    try:
        cursor.execute(_adapt_query(query, backend), args)
        if cursor.description:
            rows = [dict(row) for row in cursor.fetchall()]
            return rows[0] if one and rows else (None if one else rows)
        if not getattr(_LOCAL, "in_transaction", False):
            connection.commit()
        return None
    finally:
        cursor.close()


def query_db(query, args=(), one=False):
    active = getattr(_LOCAL, "connection", None)
    if active:
        connection, backend = active
        return _run(connection, backend, query, args, one=one)

    connection, backend = get_connection()
    try:
        result = _run(connection, backend, query, args, one=one)
        connection.commit()
        return result
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def execute_db(query, args=()):
    query_db(query, args=args, one=False)


@contextmanager
def transaction():
    if getattr(_LOCAL, "connection", None):
        yield
        return

    connection, backend = get_connection()
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
def raw_location_scope(location_id):
    """Background-job-safe equivalent of database.location_transaction() for
    the raw execute_db/query_db layer.

    get_connection()'s RLS context (_apply_request_rls_context in
    connection.py) only sets app.location_id from Flask's g object, and only
    when a Flask request context exists. Cron jobs have neither -- so any
    raw-layer call made from a background job (billing_service.py's
    close_billing_period/create_payment_link/mark_billing_paid are exactly
    this) runs with no location context at all. Under the properly
    restricted phanta_app role, RLS-protected tables like billing_records
    (see migrations/versions/0021_complete_location_rls.py) then return zero
    rows -- the same failure mode jobs/flyer_lady.py had before it was fixed
    to loop per-location with an explicit scope.

    Usage, matching the ORM's location_transaction() pattern:
        for location_id in active_location_ids:
            with raw_location_scope(location_id):
                close_billing_period(location_id=location_id)
    """
    if not isinstance(location_id, int) or location_id <= 0:
        raise ValueError("location_id must be a positive integer")

    if getattr(_LOCAL, "connection", None):
        # Already inside an outer transaction() -- set context on it and
        # let the outer block own commit/close.
        connection, backend = _LOCAL.connection
        if backend == "postgres":
            with connection.cursor() as cursor:
                cursor.execute("SELECT set_config('app.location_id', %s, true)", (str(location_id),))
                cursor.execute("SELECT set_config('app.platform_admin', '', true)")
        yield
        return

    connection, backend = get_connection()
    if backend == "postgres":
        with connection.cursor() as cursor:
            cursor.execute("SELECT set_config('app.location_id', %s, true)", (str(location_id),))
            cursor.execute("SELECT set_config('app.platform_admin', '', true)")
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


def fetch_one(query, args=()):
    return query_db(query, args=args, one=True)


def fetch_all(query, args=()):
    return query_db(query, args=args) or []