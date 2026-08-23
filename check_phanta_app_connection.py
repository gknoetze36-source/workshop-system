import os
import psycopg2

url = os.environ["PHANTA_APP_DATABASE_URL"]

conn = psycopg2.connect(url)

try:
    cur = conn.cursor()

    cur.execute("""
        SELECT
            current_database(),
            current_user,
            rolsuper,
            rolcreaterole,
            rolbypassrls,
            rolcanlogin
        FROM pg_roles
        WHERE rolname = current_user
    """)

    print("=== PHANTA APP CONNECTION ===")
    print(cur.fetchone())

    cur.execute("""
        SELECT
            schemaname,
            tablename,
            has_table_privilege(
                current_user,
                schemaname || '.' || tablename,
                'SELECT'
            ) AS can_select,
            has_table_privilege(
                current_user,
                schemaname || '.' || tablename,
                'INSERT'
            ) AS can_insert,
            has_table_privilege(
                current_user,
                schemaname || '.' || tablename,
                'UPDATE'
            ) AS can_update,
            has_table_privilege(
                current_user,
                schemaname || '.' || tablename,
                'DELETE'
            ) AS can_delete
        FROM pg_tables
        WHERE schemaname = 'public'
        ORDER BY tablename
    """)

    print()
    print("=== TABLE PRIVILEGES ===")

    for row in cur.fetchall():
        print(row)

finally:
    conn.close()
