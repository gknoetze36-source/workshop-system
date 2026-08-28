import os

from .connection import get_connection, get_connection_from_url
from .schema import _create_tables, _ensure_columns
from .migrations import run_alembic_migrations
from .indexes import _ensure_unique_username_index, _ensure_indexes
from .bootstrap import _seed_plan_defaults, _seed_saas_templates, _ensure_super_admin
from .owner_location import ensure_owner_location_foundation
from .compatibility import ensure_orm_compatibility


def initialize_database(*, run_migrations: bool = True):
    if run_migrations and os.environ.get("ADMIN_DATABASE_URL"):
        connection, backend = get_connection_from_url(
            os.environ["ADMIN_DATABASE_URL"]
        )
    else:
        connection, backend = get_connection()
    try:
        # _create_tables was previously skipped on PostgreSQL, on the assumption
        # that alembic owned the schema there. It does not: `users` is defined
        # ONLY in database/schema.py -- it is not an ORM model, so
        # Base.metadata.create_all() does not create it, and no migration
        # creates it either. The result was that a FRESH PostgreSQL database
        # could never be bootstrapped: ensure_owner_location_foundation() below
        # immediately tried to ALTER TABLE users and failed with
        # "relation users does not exist".
        #
        # This went unnoticed because an existing production database already
        # had the table, so the gap only appeared on a brand-new database --
        # which is exactly the disaster-recovery path (restore into a fresh
        # instance) that must work.
        #
        # Every statement in _create_tables is CREATE TABLE IF NOT EXISTS and is
        # backend-parameterised, so running it on PostgreSQL is idempotent and a
        # no-op against an existing database.
        _create_tables(connection, backend)
        # owners/locations must exist before _ensure_columns touches them.
        ensure_owner_location_foundation(connection, backend)
        _ensure_columns(connection, backend)
        connection.commit()
        ensure_orm_compatibility()

        if backend == "postgres" and run_migrations:
            run_alembic_migrations()

        _ensure_unique_username_index(connection, backend)
        _ensure_indexes(connection, backend)
        _seed_plan_defaults(connection, backend)
        _seed_saas_templates(connection, backend)
        _ensure_super_admin(connection, backend)
        connection.commit()
        return True
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()
