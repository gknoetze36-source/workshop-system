"""Railway pre-deploy database bootstrap and Alembic migration entrypoint.

This is intentionally a thin wrapper around initialize_database() so there is
exactly one place that defines "the database is correctly bootstrapped" —
this script and the app's own boot path (phanta_app.py) must never diverge.
"""

from .initialize import initialize_database


def main() -> None:
    initialize_database(run_migrations=True)


if __name__ == "__main__":
    main()
