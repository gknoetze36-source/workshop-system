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

def fetch_one(query, args=()):
    return query_db(query, args=args, one=True)


def fetch_all(query, args=()):
    return query_db(query, args=args) or []