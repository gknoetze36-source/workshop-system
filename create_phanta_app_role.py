import psycopg2
import os
import secrets

database_url = os.environ["DATABASE_URL"]

password = secrets.token_urlsafe(32)

c = psycopg2.connect(database_url)
c.autocommit = True
cur = c.cursor()

cur.execute("""
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_roles WHERE rolname = 'phanta_app'
    ) THEN
        CREATE ROLE phanta_app
        LOGIN
        NOSUPERUSER
        NOCREATEDB
        NOCREATEROLE
        NOINHERIT
        NOREPLICATION
        NOBYPASSRLS;
    END IF;
END
$$;
""")

cur.execute(
    "ALTER ROLE phanta_app PASSWORD %s",
    (password,)
)

cur.execute("""
SELECT
    rolname,
    rolsuper,
    rolcreaterole,
    rolcreatedb,
    rolcanlogin,
    rolreplication,
    rolbypassrls
FROM pg_roles
WHERE rolname = 'phanta_app'
""")

print("PHANTA APP ROLE:")
print(cur.fetchone())

print("\nPHANTA APP PASSWORD:")
print(password)

cur.close()
c.close()
