"""Schema compatibility bridge for the consolidated PHANTA database.

The application has a legacy/raw SQL schema and a Phase 2+ SQLAlchemy schema.
This module makes the raw schema a compatible physical representation while
preserving the public location terminology used by the existing routes.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from sqlalchemy import create_engine, inspect, text

from .connection import _database_url


def _engine():
    url = _database_url() or "sqlite:///phanta.db"
    return create_engine(
        url,
        future=True,
        pool_pre_ping=True,
        connect_args={"check_same_thread": False} if url.startswith("sqlite") else {},
    )


def _sql_type(column, dialect):
    return column.type.compile(dialect=dialect)


def ensure_orm_compatibility() -> None:
    """Create missing ORM tables/columns and ensure location-scoped ORM columns.

    Existing raw tables are never dropped or renamed. Missing ORM columns are
    added as nullable compatibility columns so existing production data remains
    readable while new ORM writes use the canonical owner/location model.
    """
    from models.core import Base
    from models import integration_models  # noqa: F401

    engine = _engine()
    with engine.begin() as conn:
        # Create all genuinely new Phase 2+ tables first using the canonical location tables.
        Base.metadata.create_all(bind=conn)
        inspector = inspect(conn)
        existing_tables = set(inspector.get_table_names())

        # Existing raw tables that also have an ORM model need every ORM column.
        for table_name, table in Base.metadata.tables.items():
            if table_name not in existing_tables:
                continue
            columns = {c["name"] for c in inspector.get_columns(table_name)}
            for column in table.columns:
                if column.name in columns:
                    continue
                # Existing production rows must remain valid; new compatibility
                # columns therefore start nullable and are populated below.
                conn.execute(text(
                    f'ALTER TABLE "{table_name}" ADD COLUMN "{column.name}" {_sql_type(column, engine.dialect)}'
                ))
            inspector = inspect(conn)

        # Translate existing raw customer/vehicle/booking data into the ORM
        # field names without removing the raw names used by the Flask routes.
        if "customers" in inspect(conn).get_table_names():
            cols = {c["name"] for c in inspect(conn).get_columns("customers")}
            if {"last_name", "surname"} <= cols:
                conn.execute(text("UPDATE customers SET last_name=surname WHERE last_name IS NULL"))
            if {"whatsapp_number", "phone"} <= cols:
                conn.execute(text("UPDATE customers SET whatsapp_number=phone WHERE whatsapp_number IS NULL"))

        if "vehicles" in inspect(conn).get_table_names():
            cols = {c["name"] for c in inspect(conn).get_columns("vehicles")}
            pairs = (("mileage", "current_mileage"), ("vin", "vehicle_vin"), ("registration", "license_plate"))
            for target, source in pairs:
                if {target, source} <= cols:
                    conn.execute(text(f"UPDATE vehicles SET {target}={source} WHERE {target} IS NULL"))

        if "bookings" in inspect(conn).get_table_names():
            cols = {c["name"] for c in inspect(conn).get_columns("bookings")}
            if {"service_type", "service"} <= cols:
                conn.execute(text("UPDATE bookings SET service_type=COALESCE(service, service_level, 'Service') WHERE service_type IS NULL"))
            if {"start_time", "scheduled_date"} <= cols:
                if engine.dialect.name == "postgresql":
                    conn.execute(text("""UPDATE bookings
                        SET start_time = CASE WHEN scheduled_date IS NOT NULL
                            THEN (scheduled_date || ' 08:00:00')::timestamp ELSE NULL END
                        WHERE start_time IS NULL"""))
                else:
                    conn.execute(text("""UPDATE bookings
                        SET start_time = CASE WHEN scheduled_date IS NOT NULL
                            THEN scheduled_date || ' 08:00:00' ELSE NULL END
                        WHERE start_time IS NULL"""))
            if {"end_time", "start_time"} <= cols:
                if engine.dialect.name == "postgresql":
                    conn.execute(text("""UPDATE bookings
                        SET end_time = CASE WHEN start_time IS NOT NULL THEN start_time + INTERVAL '1 hour' ELSE NULL END
                        WHERE end_time IS NULL"""))
                else:
                    conn.execute(text("""UPDATE bookings
                        SET end_time = CASE WHEN start_time IS NOT NULL THEN datetime(start_time, '+1 hour') ELSE NULL END
                        WHERE end_time IS NULL"""))
