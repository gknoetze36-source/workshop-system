import os


def production_requires_postgres():
    markers = (
        os.environ.get("REQUIRE_DATABASE_URL"),
        os.environ.get("RAILWAY_ENVIRONMENT"),
        os.environ.get("RAILWAY_SERVICE_ID"),
        os.environ.get("FLASK_ENV"),
        os.environ.get("APP_ENV"),
    )
    return any(str(value or "").lower() in {"1", "true", "yes", "production"} for value in markers)


def railway_pool_settings():
    return {
        "minconn": int(os.environ.get("PGPOOL_MINCONN", "1")),
        "maxconn": int(os.environ.get("PGPOOL_MAXCONN", "5")),
        "connect_timeout": int(os.environ.get("PGCONNECT_TIMEOUT", "5")),
    }
