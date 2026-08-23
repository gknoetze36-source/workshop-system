import logging
import os
from pathlib import Path

from .connection import _database_url

BASE_DIR = Path(__file__).resolve().parent.parent
logger = logging.getLogger(__name__)

def run_alembic_migrations():
    if os.environ.get("SKIP_ALEMBIC_MIGRATIONS", "").lower() in {"1", "true", "yes"}:
        return
    database_url = _database_url()
    if not database_url:
        return
    from alembic import command
    from alembic.config import Config

    config = Config(str(BASE_DIR / "alembic.ini"))
    config.set_main_option("sqlalchemy.url", database_url)
    try:
        command.upgrade(config, "head")
    except Exception:
        logger.exception("alembic_migration_failed")
        production = any(str(os.environ.get(k, "")).lower() in {"1", "true", "yes", "production"} for k in ("FLASK_ENV", "APP_ENV", "RAILWAY_ENVIRONMENT"))
        if production or os.environ.get("STRICT_ALEMBIC_MIGRATIONS", "").lower() in {"1", "true", "yes"}:
            raise
