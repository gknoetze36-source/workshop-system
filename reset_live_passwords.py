from werkzeug.security import generate_password_hash

from database import execute_db, initialize_database, utc_now


TEMP_PASSWORD = "password1234"
TARGET_USERS = ("superadmin", "demo.franchise")


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
