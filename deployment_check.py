import os
import sys
from urllib.parse import urlparse

from database import configure_database_url_from_railway_env


REQUIRED_ENV_VARS = (
    "DATABASE_URL",
    "SECRET_KEY",
    "META_APP_ID",
    "META_APP_SECRET",
    "META_ACCESS_TOKEN",
    "WHATSAPP_BUSINESS_ACCOUNT_ID",
    "WHATSAPP_PHONE_NUMBER_ID",
    "VERIFY_TOKEN",
    "OPENAI_API_KEY",
    "MESSAGING_TOKEN_ENCRYPTION_KEY",
)


def database_url_valid(value):
    parsed = urlparse(value or "")
    return parsed.scheme in {"postgres", "postgresql"} and bool(parsed.hostname) and bool(parsed.path.strip("/"))


def main():
    configure_database_url_from_railway_env()
    missing = [key for key in REQUIRED_ENV_VARS if not os.environ.get(key)]
    if missing:
        print("Missing required environment variables:")
        for key in missing:
            print(f"- {key}")
        return 1

    if not database_url_valid(os.environ.get("DATABASE_URL")):
        print("DATABASE_URL must be a valid postgres/postgresql URL.")
        return 1

    print("Deployment configuration check passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
