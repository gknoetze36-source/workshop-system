import logging
import os
import sqlite3
import threading

from urllib.parse import quote, urlsplit, urlunsplit

logger = logging.getLogger(__name__)
PRIMARY_SQLITE_PATH = os.environ.get("PHANTA_SQLITE_PATH", "phanta.db")

def configure_database_url_from_railway_env():
    if os.environ.get("DATABASE_URL"):
        return os.environ["DATABASE_URL"]

    required = ("PGHOST", "PGPORT", "PGDATABASE", "PGUSER", "PGPASSWORD")
    values = {key: os.environ.get(key) for key in required}
    if not all(values.values()):
        return ""

    user = quote(values["PGUSER"], safe="")
    password = quote(values["PGPASSWORD"], safe="")
    host = values["PGHOST"]
    port = values["PGPORT"]
    database = quote(values["PGDATABASE"], safe="")
    database_url = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    os.environ["DATABASE_URL"] = database_url
    return database_url


def require_postgres_for_service():
    production_markers = (
        os.environ.get("REQUIRE_DATABASE_URL"),
        os.environ.get("RAILWAY_ENVIRONMENT"),
        os.environ.get("RAILWAY_SERVICE_ID"),
        os.environ.get("FLASK_ENV"),
        os.environ.get("APP_ENV"),
    )
    return any(str(value or "").lower() in {"1", "true", "yes", "production"} for value in production_markers)


_POOL = None
_POOL_LOCK = threading.Lock()
_LOCAL = threading.local()


def _database_url():
    configure_database_url_from_railway_env()
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        return database_url

    parsed = urlsplit(database_url)
    if parsed.scheme not in {"postgres", "postgresql"}:
        return database_url

    path = parsed.path or ""
    for marker in ("postgresql://", "postgres://"):
        marker_index = path.find(marker)
        if marker_index > 0:
            fixed_path = path[:marker_index].rstrip("/")
            logger.warning("database_url_had_concatenated_postgres_url; using database path before duplicate scheme")
            return urlunsplit((parsed.scheme, parsed.netloc, fixed_path or "/railway", parsed.query, parsed.fragment))
    return database_url


class _PooledConnection:
    def __init__(self, pool, connection):
        self._pool = pool
        self._connection = connection
        self._closed = False

    def __getattr__(self, name):
        return getattr(self._connection, name)

    def close(self):
        if not self._closed:
            self._pool.putconn(self._connection)
            self._closed = True


def _postgres_pool():
    global _POOL
    if _POOL is None:
        with _POOL_LOCK:
            if _POOL is None:
                from psycopg2.pool import ThreadedConnectionPool

                database_url = _database_url()
                minconn = int(os.environ.get("PGPOOL_MINCONN", "1"))
                maxconn = int(os.environ.get("PGPOOL_MAXCONN", "5"))
                timeout = int(os.environ.get("PGCONNECT_TIMEOUT", "5"))
                _POOL = ThreadedConnectionPool(minconn, maxconn, database_url, connect_timeout=timeout)
    return _POOL


def _apply_request_rls_context(connection, backend):
    """Bind the current Flask request identity to a PostgreSQL transaction.

    The application uses forced RLS. SQLAlchemy sessions are handled in
    database.sqlalchemy_session, while these legacy/raw query helpers need the
    same transaction-local context here. Background jobs and provider ingress
    without a location must use an explicit location/platform context before
    touching location-owned records.
    """
    if backend != "postgres":
        return
    try:
        from flask import g, has_request_context
        if not has_request_context():
            return
        location_id = getattr(g, "location_id", None)
        platform_admin = bool(getattr(g, "platform_admin", False))
    except RuntimeError:
        return

    with connection.cursor() as cursor:
        cursor.execute("SELECT set_config('app.location_id', %s, true)", (str(location_id) if isinstance(location_id, int) and location_id > 0 else "",))
        cursor.execute("SELECT set_config('app.platform_admin', %s, true)", ("1" if platform_admin else "",))


def get_connection_from_url(database_url):
    if not database_url:
        raise RuntimeError("database_url is required")

    if database_url.startswith(("postgres://", "postgresql://")):
        import psycopg2

        connection = psycopg2.connect(
            database_url,
            connect_timeout=int(os.environ.get("PGCONNECT_TIMEOUT", "5")),
        )
        connection.autocommit = False
        return connection, "postgres"

    connection = sqlite3.connect(database_url)
    connection.row_factory = sqlite3.Row
    return connection, "sqlite"


def get_connection():
    database_url = _database_url()
    if database_url:
        pool = _postgres_pool()
        connection = _PooledConnection(pool, pool.getconn())
        connection.autocommit = False
        _apply_request_rls_context(connection, "postgres")
        return connection, "postgres"

    if require_postgres_for_service():
        raise RuntimeError("DATABASE_URL is required for this Railway service. Add DATABASE_URL=${{Postgres.DATABASE_URL}} to the service variables.")

    connection = sqlite3.connect(PRIMARY_SQLITE_PATH)
    connection.row_factory = sqlite3.Row
    return connection, "sqlite"


