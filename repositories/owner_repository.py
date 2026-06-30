"""
Owner Repository for Workshop System Version 2.

This repository handles all database operations for the Owner entity.
It interacts with the existing 'franchises' table as a placeholder
until the migration to the Owner table is performed.
"""

from database import query_db, execute_db


def get_owner_by_id(owner_id):
    """Retrieve an owner by their ID."""
    sql = "SELECT * FROM franchises WHERE id = %s"
    return query_db(sql, (owner_id,), one=True)


def get_owner_by_slug(slug):
    """Retrieve an owner by their slug."""
    sql = "SELECT * FROM franchises WHERE slug = %s"
    return query_db(sql, (slug,), one=True)


def list_owners(limit=None, offset=None):
    """List owners with optional pagination."""
    sql = "SELECT * FROM franchises ORDER BY name"
    params = []
    if limit is not None:
        sql += " LIMIT %s"
        params.append(limit)
    if offset is not None:
        sql += " OFFSET %s"
        params.append(offset)
    return query_db(sql, params)


def create_owner(owner_data):
    """Create a new owner. Returns the new owner's ID."""
    columns = ', '.join(owner_data.keys())
    placeholders = ', '.join(['%s'] * len(owner_data))
    sql = f"INSERT INTO franchises ({columns}) VALUES ({placeholders})"
    execute_db(sql, tuple(owner_data.values()))
    # Return the ID of the newly inserted row
    # Note: In PostgreSQL, we would use RETURNING id, but we are using the existing
    # database.py which may not support that. We'll use a separate query to get the last insert id.
    # For SQLite, we can use lastrowid, but we are using database.py which abstracts.
    # Since we don't have a way to get the last inserted id from database.py, we'll do:
    return query_db("SELECT last_insert_rowid() AS id", one=True)['id']


def update_owner(owner_id, owner_data):
    """Update an existing owner."""
    set_clause = ', '.join([f"{key} = %s" for key in owner_data.keys()])
    sql = f"UPDATE franchises SET {set_clause} WHERE id = %s"
    params = tuple(owner_data.values()) + (owner_id,)
    execute_db(sql, params)


def delete_owner(owner_id):
    """Delete an owner by ID."""
    sql = "DELETE FROM franchises WHERE id = %s"
    execute_db(sql, (owner_id,))


def owner_exists(owner_id):
    """Check if an owner exists by ID."""
    sql = "SELECT 1 FROM franchises WHERE id = %s LIMIT 1"
    return query_db(sql, (owner_id,), one=True) is not None


def get_owner_count():
    """Get the total number of owners."""
    sql = "SELECT COUNT(*) AS count FROM franchises"
    result = query_db(sql, one=True)
    return result['count'] if result else 0
