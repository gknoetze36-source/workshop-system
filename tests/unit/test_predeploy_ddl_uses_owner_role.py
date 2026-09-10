"""Pre-deploy DDL must run as the table-owning role, not the app role.

Regression test for the 2026-09-08 production outage: the phanta-web
deployment failed at PRE_DEPLOY_COMMAND with

    psycopg2.errors.InsufficientPrivilege:
    must be owner of table meta_social_oauth_sessions
    [SQL: ALTER TABLE "meta_social_oauth_sessions"
          ADD COLUMN "meta_user_id" VARCHAR(100)]

initialize_database() opened its own connection with ADMIN_DATABASE_URL, but
ensure_orm_compatibility() built a separate engine from plain DATABASE_URL and
ran all of its DDL as the least-privilege app role. On Railway that role does
not own the tables, so the first new ORM column took the whole deployment down.

The two must stay on the same role. These tests fail if anyone reverts
ensure_orm_compatibility() to resolving its own connection.
"""
import os
from unittest.mock import MagicMock, patch

from database import initialize as initialize_module


ADMIN_URL = "postgresql://owner:secret@db.internal:5432/phanta"
APP_URL = "postgresql://app:secret@db.internal:5432/phanta"


def _run_initialize(env):
    """Run initialize_database with the DB work stubbed, capturing the URL
    handed to ensure_orm_compatibility."""
    captured = {}

    def fake_compat(database_url=None):
        captured["url"] = database_url

    with patch.dict(os.environ, env, clear=False), \
         patch.object(initialize_module, "ensure_orm_compatibility", fake_compat), \
         patch.object(initialize_module, "get_connection_from_url") as from_url, \
         patch.object(initialize_module, "get_connection") as plain, \
         patch.object(initialize_module, "_create_tables"), \
         patch.object(initialize_module, "ensure_owner_location_foundation"), \
         patch.object(initialize_module, "_ensure_columns"), \
         patch.object(initialize_module, "run_alembic_migrations"), \
         patch.object(initialize_module, "_ensure_unique_username_index"), \
         patch.object(initialize_module, "_ensure_indexes"), \
         patch.object(initialize_module, "_seed_plan_defaults"), \
         patch.object(initialize_module, "_seed_saas_templates"), \
         patch.object(initialize_module, "_ensure_super_admin"):
        from_url.return_value = (MagicMock(name="admin_connection"), "postgres")
        plain.return_value = (MagicMock(name="app_connection"), "postgres")
        initialize_module.initialize_database(run_migrations=True)
        return captured, from_url, plain


def test_compatibility_ddl_receives_the_admin_url():
    captured, from_url, _ = _run_initialize({
        "ADMIN_DATABASE_URL": ADMIN_URL,
        "DATABASE_URL": APP_URL,
    })
    assert captured["url"] == ADMIN_URL, (
        "ensure_orm_compatibility() must run its ALTER TABLE statements as the "
        "owner role from ADMIN_DATABASE_URL, not the app role"
    )
    from_url.assert_called_once_with(ADMIN_URL)


def test_without_an_admin_url_compatibility_falls_back_to_the_default_engine():
    env = {"DATABASE_URL": APP_URL}
    os.environ.pop("ADMIN_DATABASE_URL", None)
    captured, _, plain = _run_initialize(env)
    assert captured["url"] is None, (
        "with no ADMIN_DATABASE_URL, ensure_orm_compatibility() should resolve "
        "the ordinary connection itself rather than being handed a URL"
    )
    plain.assert_called_once()


def test_compatibility_engine_accepts_an_explicit_url():
    """The engine helper must honour the URL it is given rather than always
    re-reading DATABASE_URL from the environment."""
    from database.compatibility import _engine

    engine = _engine("sqlite:///:memory:")
    assert str(engine.url) == "sqlite:///:memory:"
