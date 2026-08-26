from __future__ import annotations
import os
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from logging.config import fileConfig
from alembic import context
from sqlalchemy import engine_from_config, pool
from models.core import Base
import flyer_lady.models
from models import integration_models  # noqa: F401

config = context.config
if config.config_file_name:
    fileConfig(config.config_file_name)
target_metadata = Base.metadata

def get_url():
    return config.get_main_option("sqlalchemy.url") or os.getenv("DATABASE_URL")

def run_migrations_offline():
    context.configure(url=get_url(), target_metadata=target_metadata, literal_binds=True, compare_type=True)
    with context.begin_transaction():
        context.run_migrations()

def _ensure_wide_version_column():
    """Alembic hardcodes alembic_version.version_num as VARCHAR(32). This
    project's revision IDs exceed that (e.g. "0009_booking_confirmation_
    phase15" is 34 chars), which fails the first UPDATE once the chain
    reaches one. Widen/create the column in its own autocommitted
    connection, fully separate from and committed before the migration
    connection Alembic manages below -- mixing raw DDL into that connection
    caused the whole run to silently roll back (nothing after it persisted,
    including alembic_version itself) the first time this was attempted.
    """
    url = get_url()
    if not url or not url.startswith("postgresql"):
        return
    from sqlalchemy import create_engine, inspect as sa_inspect
    engine = create_engine(url, isolation_level="AUTOCOMMIT")
    try:
        with engine.connect() as conn:
            if "alembic_version" in sa_inspect(conn).get_table_names():
                conn.exec_driver_sql(
                    "ALTER TABLE alembic_version ALTER COLUMN version_num TYPE VARCHAR(255)"
                )
            else:
                conn.exec_driver_sql(
                    "CREATE TABLE alembic_version (version_num VARCHAR(255) NOT NULL, "
                    "CONSTRAINT alembic_version_pkc PRIMARY KEY (version_num))"
                )
    finally:
        engine.dispose()


def run_migrations_online():
    _ensure_wide_version_column()
    configuration = config.get_section(config.config_ini_section) or {}
    configuration["sqlalchemy.url"] = get_url()
    connectable = engine_from_config(configuration, prefix="sqlalchemy.", poolclass=pool.NullPool)
    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata, compare_type=True)
        with context.begin_transaction():
            context.run_migrations()

if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
