"""
User Repository for Workshop System Version 2.

This repository handles all database operations for the User entity.
It interacts with the existing 'users' table.
"""

from database import query_db


def get_user_by_username_or_email(username_or_email):
    """Retrieve a user by username or email (case-insensitive)."""
    sql = "SELECT * FROM users WHERE lower(username)=lower(%s) OR lower(email)=lower(%s)"
    return query_db(sql, (username_or_email, username_or_email), one=True)


def get_user_by_username(username):
    """Retrieve a user by username (case-insensitive)."""
    sql = "SELECT * FROM users WHERE lower(username)=lower(%s)"
    return query_db(sql, (username,), one=True)


def get_user_by_id(user_id):
    """Retrieve a user by their ID."""
    sql = "SELECT * FROM users WHERE id=%s"
    return query_db(sql, (user_id,), one=True)


def get_user_count_by_franchise(franchise_id):
    """Return the number of active users in a franchise."""
    sql = "SELECT COUNT(*) AS total FROM users WHERE franchise_id=%s AND COALESCE(active, TRUE)=TRUE"
    result = query_db(sql, (franchise_id,), one=True)
    return result['total'] if result else 0


def get_user_counts_by_franchise():
    """Return a list of user counts per franchise."""
    sql = "SELECT franchise_id, COUNT(*) AS total FROM users WHERE COALESCE(active, TRUE)=TRUE GROUP BY franchise_id"
    return query_db(sql)


def get_users_with_filters(scope_sql, args):
    """
    Retrieve users with arbitrary scope SQL and args.
    Expected to be used with complex filtering (e.g., manage users).
    """
    sql = f"SELECT u.*, f.name AS franchise_name, b.name AS branch_name FROM users u LEFT JOIN franchises f ON f.id = u.franchise_id LEFT JOIN branches b ON b.id = u.branch_id WHERE {scope_sql} ORDER BY u.role, u.username"
    return query_db(sql, args)


def get_non_superadmin_excluding_current(current_username):
    """Return all non-superadmin users except the current user."""
    sql = "SELECT * FROM users WHERE role <> 'super_admin' AND lower(username)<>lower(%s)"
    return query_db(sql, (current_username,))


def get_all_users_ordered_by_franchise_name():
    """Return all users ordered by franchise name then username."""
    sql = "SELECT u.*, f.name AS franchise_name, b.name AS branch_name FROM users u LEFT JOIN franchises f ON f.id = u.franchise_id LEFT JOIN branches b ON b.id = u.branch_id ORDER BY f.name, u.username"
    return query_db(sql)


def get_users_except_superadmin_with_username(username):
    """Return all users except the superadmin user with the given username."""
    sql = "SELECT * FROM users WHERE NOT (role = 'super_admin' AND username = %s)"
    return query_db(sql, (username,))


def user_exists(username_or_email):
    """Check if a user exists by username or email."""
    return get_user_by_username_or_email(username_or_email) is not None
