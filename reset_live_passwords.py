from werkzeug.security import generate_password_hash

from database import execute_db, initialize_database, utc_now

import os
import secrets
def generate_secure_temp_password():
    """Generate a cryptographically secure temporary password."""
    return secrets.token_urlsafe(18)
TEMP_PASSWORD = os.environ.get("TEMP_PASSWORD") or generate_secure_temp_password()
TARGET_USERS = tuple(u.strip() for u in (os.environ.get("TARGET_USERS", "superadmin,demo.franchise").split(",")))


def main():
    state = initialize_database()
    print(f"Database ready: {state['backend']}")
    password_hash = generate_password_hash(TEMP_PASSWORD)
    for username in TARGET_USERS:
        execute_db(
            """
            UPDATE users
            SET password_hash=%s,
                password=%s,
                must_reset_password=1,
                updated_at=%s
            WHERE lower(username)=lower(%s)
            """,
            (password_hash, "", utc_now(), username),
        )
        print(f"reset:{username}")


if __name__ == "__main__":
    main()
