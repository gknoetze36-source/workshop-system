"""
Location Repository for Workshop System Version 2.

This repository handles all database operations for the Location entity.
It interacts with the existing 'branches' table.
"""

from database import query_db


def get_visible_branches(user=None, franchise_id=None, include_inactive=False, public_only=False):
    """Return a list of visible branches based on user, franchise_id, include_inactive, public_only flags."""
    clauses = []
    args = []
    if not include_inactive:
        clauses.append("b.active = TRUE")
    if public_only:
        clauses.append("b.public_booking_enabled = TRUE")
    if user:
        if user["role"] == "reception":
            clauses.append("b.id = %s")
            args.append(user["branch_id"])
        elif user["role"] == "franchise_admin":
            clauses.append("b.franchise_id = %s")
            args.append(user["franchise_id"])
    if franchise_id:
        clauses.append("b.franchise_id = %s")
        args.append(franchise_id)

    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    return query_db(
        """
        SELECT
            b.*,
            f.name AS franchise_name,
            f.slug AS franchise_slug
        FROM branches b
        LEFT JOIN franchises f ON f.id = b.franchise_id
        """
        + where
        + " ORDER BY f.name, b.name",
        tuple(args),
    )
