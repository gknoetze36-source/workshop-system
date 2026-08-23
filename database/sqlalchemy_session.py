"""Database engine and session helpers for PHANTA."""

from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Iterator

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker

from models.core import Base
from models import integration_models  # noqa: F401 - registers integration tables
import flyer_lady.models  # noqa: F401 - registers Flyer Lady tables


def get_database_url() -> str:
    """Return the configured DB URL; production services must use PostgreSQL."""
    url = os.getenv("DATABASE_URL")
    production = any(str(os.getenv(k, "")).lower() in {"1", "true", "yes", "production"} for k in ("FLASK_ENV", "APP_ENV", "RAILWAY_ENVIRONMENT"))
    if production and not url:
        raise RuntimeError("DATABASE_URL is required in production")
    return url or "sqlite:///phanta.db"


def create_db_engine():
    url = get_database_url()
    connect_args = {"check_same_thread": False} if url.startswith("sqlite") else {}
    return create_engine(url, future=True, pool_pre_ping=True, connect_args=connect_args)


print("=== DATABASE DEBUG ===")
print("DATABASE_URL_PRESENT:", bool(os.getenv("DATABASE_URL")))
print("DATABASE_URL_LENGTH:", len(os.getenv("DATABASE_URL", "")))
print("FLASK_ENV:", os.getenv("FLASK_ENV"))
print("APP_ENV:", os.getenv("APP_ENV"))
print("RAILWAY_ENVIRONMENT:", os.getenv("RAILWAY_ENVIRONMENT"))
print("======================")

engine = create_db_engine()
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)

def init_db() -> None:
    """Create missing tables.

    Production deployments should use Alembic migrations rather than relying
    on create_all(). This helper is intentionally useful for local smoke tests.
    """
    Base.metadata.create_all(bind=engine)


def get_session() -> Session:
    """Return a session with the current Flask location bound when available.

    Normal authenticated web requests historically used this helper directly.
    On PostgreSQL that must still establish ``app.location_id`` before any
    location-owned query because RLS is forced. Background jobs and provider
    ingress without a location context remain unscoped and must use their
    dedicated resolution flow before touching location-owned data.
    Platform-admin reads use ``get_platform_session`` instead.
    """
    session = SessionLocal()
    try:
        from flask import g, has_request_context
        location_id = getattr(g, "location_id", None) if has_request_context() else None
    except RuntimeError:
        location_id = None
    if isinstance(location_id, int) and location_id > 0:
        set_location_id(session, location_id)
    return session


def get_platform_session() -> Session:
    """Return a read-only platform-admin session with explicit RLS context.

    This does not bypass PostgreSQL RLS. A migration adds a SELECT-only
    platform policy keyed by ``app.platform_admin``. The flag is LOCAL to
    the transaction and therefore cannot leak through a pooled connection.
    """
    session = SessionLocal()
    if session.bind and session.bind.dialect.name == "postgresql":
        session.execute(text("SELECT set_config('app.platform_admin', '1', true)"))
    return session


@contextmanager
def session_scope(location_id: int | None = None) -> Iterator[Session]:
    """Open one transaction; when location_id is supplied, bind the PostgreSQL RLS context."""
    session = SessionLocal()
    try:
        if location_id is not None:
            set_location_id(session, location_id)
        elif session.bind and session.bind.dialect.name == "postgresql":
            # Fail closed for application transactions. Provider-ingress code that
            # cannot know the location yet must use its dedicated ingress transaction
            # and resolve the location before touching location-owned records.
            session.execute(text("SELECT set_config('app.location_id', '', true)"))
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()

@contextmanager
def location_transaction(location_id: int) -> Iterator[Session]:
    if not isinstance(location_id, int) or location_id <= 0:
        raise ValueError("location_id must be a positive integer")
    with session_scope(location_id=location_id) as session:
        yield session


def set_location_id(session: Session, location_id: int) -> None:
    """Set the PostgreSQL RLS location for the current transaction."""
    if session.bind and session.bind.dialect.name == "postgresql":
        session.execute(
            text("SELECT set_config('app.location_id', :location_id, true)"),
            {"location_id": str(location_id)},
        )
