"""
User Repository for Workshop System Version 2.

This repository handles all database operations for the User entity.
"""

from database import query_db


# ============================================================================
# Shared SQL
# ============================================================================

_BASE_USER_SELECT = """
    SELECT
        u.*,
        f.name AS location_name,
        b.name AS location_name
    FROM users u
    LEFT JOIN locations f
        ON f.id = u.location_id
    LEFT JOIN locations b
        ON b.id = u.location_id
"""


# ============================================================================
# Internal Helpers
# ============================================================================

def _get_user_by_field(field, value):
    """
    Generic user lookup helper.
    """

    allowed_fields = {
        "id",
        "username",
        "email",
    }

    if field not in allowed_fields:
        raise ValueError(f"Unsupported user lookup field: {field}")

    if field == "id":
        sql = """
            SELECT *
            FROM users
            WHERE id = %s
            LIMIT 1
        """
        params = (value,)
    else:
        sql = f"""
            SELECT *
            FROM users
            WHERE lower({field}) = lower(%s)
            LIMIT 1
        """
        params = (value,)

    return query_db(sql, params, one=True)


# ============================================================================
# User Lookups
# ============================================================================

def get_user_by_username_or_email(username_or_email):
    """Return a user by username or email."""

    sql = """
        SELECT *
        FROM users
        WHERE lower(username)=lower(%s)
           OR lower(email)=lower(%s)
        LIMIT 1
    """

    return query_db(
        sql,
        (username_or_email, username_or_email),
        one=True,
    )


def get_user_by_username(username):
    """Return a user by username."""

    return _get_user_by_field(
        "username",
        username,
    )


def get_user_by_id(user_id):
    """Return a user by ID."""

    return _get_user_by_field(
        "id",
        user_id,
    )


# ============================================================================
# Statistics
# ============================================================================

def get_user_count_by_location(location_id):
    """Return the number of active users."""

    sql = """
        SELECT COUNT(*) AS total
        FROM users
        WHERE location_id=%s
          AND COALESCE(active, TRUE)=TRUE
    """

    result = query_db(
        sql,
        (location_id,),
        one=True,
    )

    return result["total"] if result else 0


def get_user_counts_by_location():
    """Return active user totals grouped by location."""

    sql = """
        SELECT
            location_id,
            COUNT(*) AS total
        FROM users
        WHERE COALESCE(active, TRUE)=TRUE
        GROUP BY location_id
    """

    return query_db(sql)


# ============================================================================
# User Lists
# ============================================================================

def get_users_with_filters(scope_sql, args):
    """
    Retrieve users using caller-supplied scope SQL.

    NOTE:
    This function intentionally preserves its public API for backwards
    compatibility. Validation of scope_sql should occur in the service layer.
    """

    sql = f"""
        {_BASE_USER_SELECT}
        WHERE {scope_sql}
        ORDER BY u.role, u.username
    """

    return query_db(sql, tuple(args))


def get_non_superadmin_excluding_current(current_username):
    """Return non-superadmin users excluding the current user."""

    sql = """
        SELECT *
        FROM users
        WHERE role <> 'super_admin'
          AND lower(username) <> lower(%s)
    """

    return query_db(
        sql,
        (current_username,),
    )


def get_all_users_ordered_by_location_name():
    """Return all users ordered by location."""

    sql = f"""
        {_BASE_USER_SELECT}
        ORDER BY f.name, u.username
    """

    return query_db(sql)


def get_users_except_superadmin_with_username(username):
    """Return all users except the matching superadmin."""

    sql = """
        SELECT *
        FROM users
        WHERE NOT (
            role='super_admin'
            AND username=%s
        )
    """

    return query_db(
        sql,
        (username,),
    )


# ============================================================================
# Utilities
# ============================================================================

def user_exists(username_or_email):
    """Return True if a user exists."""

    return (
        get_user_by_username_or_email(username_or_email)
        is not None
    )