"""Canonical database package for PHANTA.

Exposes both the legacy query helpers used by the Flask application and the
SQLAlchemy session helpers used by location-scoped Phase 2+ integrations.
"""

from .connection import get_connection, require_postgres_for_service
from .query import query_db, execute_db, fetch_one, fetch_all, transaction
from .utils import (
    utc_now,
    slugify,
    parse_any_date,
    iso_date,
    classify_service_level,
)
from .sqlalchemy_session import (
    Base,
    engine,
    SessionLocal,
    get_session,
    init_db,
    session_scope,
    location_transaction,
    set_location_id,
    get_platform_session,
)
from .initialize import initialize_database

__all__ = [
    "Base", "engine", "SessionLocal", "get_session", "init_db",
    "session_scope", "location_transaction", "set_location_id", "get_platform_session",
    "get_connection", "require_postgres_for_service",
    "query_db", "execute_db", "fetch_one", "fetch_all", "transaction",
    "utc_now", "slugify", "parse_any_date", "iso_date",
    "classify_service_level", "initialize_database",
]
