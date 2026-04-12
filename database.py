import os
import sqlite3

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PRIMARY_SQLITE_PATH = os.environ.get("SQLITE_PATH") or os.path.join(BASE_DIR, "database.db")
FALLBACK_SQLITE_PATH = os.path.join(BASE_DIR, "database.local.db")
ACTIVE_SQLITE_PATH = PRIMARY_SQLITE_PATH

USERS_COLUMNS = {
    "username": "TEXT UNIQUE",
    "password": "TEXT",
    "branch": "TEXT",
    "role": "TEXT",
    "company": "TEXT DEFAULT 'MAIN'",
}

BOOKINGS_COLUMNS = {
    "first_name": "TEXT",
    "surname": "TEXT",
    "phone": "TEXT",
    "make": "TEXT",
    "model": "TEXT",
    "service": "TEXT",
    "date": "TEXT",
    "branch": "TEXT",
    "status": "TEXT DEFAULT 'Pending'",
    "work_to_be_done": "TEXT",
    "source": "TEXT",
    "quote_declined": "TEXT DEFAULT 'No'",
    "contacted": "TEXT DEFAULT 'No'",
    "company": "TEXT DEFAULT 'MAIN'",
}


def _sqlite_candidates():
    paths = [ACTIVE_SQLITE_PATH, PRIMARY_SQLITE_PATH]
    if not os.environ.get("SQLITE_PATH"):
        paths.append(FALLBACK_SQLITE_PATH)

    unique_paths = []
    for path in paths:
        if path and path not in unique_paths:
            unique_paths.append(path)
    return unique_paths


def _open_sqlite_connection(path):
    connection = sqlite3.connect(path)
    connection.row_factory = sqlite3.Row
    return connection


def get_connection():
    database_url = os.environ.get("DATABASE_URL")
    if database_url:
        import psycopg2

        return psycopg2.connect(database_url), "postgres"

    return _open_sqlite_connection(ACTIVE_SQLITE_PATH), "sqlite"


def _adapt_query(query, backend):
    if backend == "sqlite":
        return query.replace("%s", "?")
    return query


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

        connection.commit()
        return None
    finally:
        cursor.close()


def _run_sqlite(query, args=(), one=False):
    global ACTIVE_SQLITE_PATH

    last_error = None
    for path in _sqlite_candidates():
        connection = None
        try:
            connection = _open_sqlite_connection(path)
            result = _run(connection, "sqlite", query, args, one=one)
            ACTIVE_SQLITE_PATH = path
            return result
        except sqlite3.Error as exc:
            last_error = exc
        finally:
            if connection is not None:
                connection.close()

    raise last_error


def query_db(query, args=(), one=False):
    if os.environ.get("DATABASE_URL"):
        connection, backend = get_connection()
        try:
            return _run(connection, backend, query, args, one=one)
        finally:
            connection.close()

    return _run_sqlite(query, args=args, one=one)


def _get_columns(connection, backend, table):
    if backend == "postgres":
        cursor = connection.cursor()
        try:
            cursor.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = %s
                """,
                (table,),
            )
            return {row[0] for row in cursor.fetchall()}
        finally:
            cursor.close()

    cursor = connection.execute(f"PRAGMA table_info({table})")
    return {row[1] for row in cursor.fetchall()}


def _create_tables(connection, backend):
    if backend == "postgres":
        _run(
            connection,
            backend,
            """
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                username TEXT UNIQUE,
                password TEXT,
                branch TEXT,
                role TEXT,
                company TEXT DEFAULT 'MAIN'
            )
            """,
        )
        _run(
            connection,
            backend,
            """
            CREATE TABLE IF NOT EXISTS bookings (
                id SERIAL PRIMARY KEY,
                first_name TEXT,
                surname TEXT,
                phone TEXT,
                make TEXT,
                model TEXT,
                service TEXT,
                date TEXT,
                branch TEXT,
                status TEXT DEFAULT 'Pending',
                work_to_be_done TEXT,
                source TEXT,
                quote_declined TEXT DEFAULT 'No',
                contacted TEXT DEFAULT 'No',
                company TEXT DEFAULT 'MAIN'
            )
            """,
        )
        return

    _run(
        connection,
        backend,
        """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE,
            password TEXT,
            branch TEXT,
            role TEXT,
            company TEXT DEFAULT 'MAIN'
        )
        """,
    )
    _run(
        connection,
        backend,
        """
        CREATE TABLE IF NOT EXISTS bookings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            first_name TEXT,
            surname TEXT,
            phone TEXT,
            make TEXT,
            model TEXT,
            service TEXT,
            date TEXT,
            branch TEXT,
            status TEXT DEFAULT 'Pending',
            work_to_be_done TEXT,
            source TEXT,
            quote_declined TEXT DEFAULT 'No',
            contacted TEXT DEFAULT 'No',
            company TEXT DEFAULT 'MAIN'
        )
        """,
    )


def _ensure_columns(connection, backend, table, columns):
    existing_columns = _get_columns(connection, backend, table)
    for column_name, definition in columns.items():
        if column_name in existing_columns:
            continue
        _run(
            connection,
            backend,
            f"ALTER TABLE {table} ADD COLUMN {column_name} {definition}",
        )


def _default_field(connection, backend, table, column_name, value):
    _run(
        connection,
        backend,
        f"""
        UPDATE {table}
        SET {column_name} = %s
        WHERE {column_name} IS NULL OR {column_name} = ''
        """,
        (value,),
    )


def _backfill_from_legacy(connection, backend):
    booking_columns = _get_columns(connection, backend, "bookings")

    if "customer" in booking_columns:
        _run(
            connection,
            backend,
            """
            UPDATE bookings
            SET first_name = COALESCE(NULLIF(first_name, ''), customer)
            WHERE customer IS NOT NULL AND customer <> ''
            """,
        )

    if "vehicle" in booking_columns:
        _run(
            connection,
            backend,
            """
            UPDATE bookings
            SET make = COALESCE(NULLIF(make, ''), vehicle)
            WHERE vehicle IS NOT NULL AND vehicle <> ''
            """,
        )


def _seed_sample_users(connection, backend):
    if os.environ.get("SEED_SAMPLE_USERS", "").lower() != "true":
        return

    users = [
        ("admin", "1234", "ALL", "admin", "MAIN"),
        ("silverton", "1234", "Silverton", "staff", "MAIN"),
    ]

    for username, password, branch, role, company in users:
        existing_user = _run(
            connection,
            backend,
            "SELECT id FROM users WHERE username = %s",
            (username,),
            one=True,
        )
        if existing_user:
            continue
        _run(
            connection,
            backend,
            """
            INSERT INTO users (username, password, branch, role, company)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (username, password, branch, role, company),
        )


def _initialize(connection, backend):
    _create_tables(connection, backend)
    _ensure_columns(connection, backend, "users", USERS_COLUMNS)
    _ensure_columns(connection, backend, "bookings", BOOKINGS_COLUMNS)
    _backfill_from_legacy(connection, backend)

    _default_field(connection, backend, "users", "company", "MAIN")

    for column_name, value in {
        "surname": "",
        "phone": "",
        "make": "",
        "model": "",
        "service": "",
        "branch": "",
        "status": "Pending",
        "work_to_be_done": "",
        "source": "Booking",
        "quote_declined": "No",
        "contacted": "No",
        "company": "MAIN",
    }.items():
        _default_field(connection, backend, "bookings", column_name, value)

    _seed_sample_users(connection, backend)


def initialize_database():
    global ACTIVE_SQLITE_PATH

    if os.environ.get("DATABASE_URL"):
        connection, backend = get_connection()
        try:
            _initialize(connection, backend)
            return None
        finally:
            connection.close()

    last_error = None
    for path in _sqlite_candidates():
        connection = None
        try:
            connection = _open_sqlite_connection(path)
            _initialize(connection, "sqlite")
            ACTIVE_SQLITE_PATH = path
            return path
        except sqlite3.Error as exc:
            last_error = exc
        finally:
            if connection is not None:
                connection.close()

    raise last_error


if __name__ == "__main__":
    active_path = initialize_database()
    print(f"Database ready: {active_path or 'postgres'}")
