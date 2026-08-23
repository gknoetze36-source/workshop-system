# Canonical Database Layer

`database/` is the authoritative database package for PHANTA/VANTA.

There is intentionally no top-level `database.py` module. Python resolves
`from database import ...` to this package's `database/__init__.py`.

The package is the single compatibility boundary between:

1. Existing Flask/legacy SQL helpers
2. SQLAlchemy sessions/models
3. PostgreSQL production connections
4. Local SQLite development
5. Alembic migrations
6. Schema bootstrap/compatibility

## Public API

`database/__init__.py` exports:

- `get_connection`
- `require_postgres_for_service`
- `query_db`
- `execute_db`
- `fetch_one`
- `fetch_all`
- `transaction`
- `utc_now`
- `slugify`
- `parse_any_date`
- `iso_date`
- `classify_service_level`
- `Base`
- `engine`
- `SessionLocal`
- `get_session`
- `init_db`
- `session_scope`
- `tenant_transaction`
- `set_tenant_id`
- `get_platform_session`
- `initialize_database`

## Startup flow

Local/application startup:

    phanta_app.py
      -> database.initialize_database(run_migrations=False)
      -> database package
      -> connection + schema + compatibility

Railway pre-deploy:

    python -m database.predeploy
      -> connection
      -> legacy schema bootstrap
      -> ORM compatibility
      -> Alembic (PostgreSQL)
      -> commit

Production application:

    from database import get_session
    from database import query_db, execute_db

## Why this resolves the previous finding

A filename search for `database.py` was the wrong test for Python's import
contract. The supplied application contains a complete `database/` package
with `__init__.py`, and Python's import system resolves `import database` to
that package.

The authoritative layer is therefore the package, not a missing module.

Do not add a sibling `database.py`; that would create an unnecessary naming
collision and could make import behavior ambiguous.

## Phase 3 rule

New universal domain code should depend on this package for persistence.
Legacy Workshop repositories may continue using the compatibility helpers
during the migration period, but no second database abstraction should be
introduced.
