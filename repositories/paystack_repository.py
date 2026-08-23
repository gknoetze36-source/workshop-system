"""
Paystack Repository for Workshop System Version 2.

This repository handles all database operations for Paystack webhook events.
"""

from database import query_db, execute_db, utc_now


DEFAULT_PENDING_LIMIT = 100


def create_webhook_event(event_id, reference, event_type, owner_id, payload_json):
    """
    Create a new Paystack webhook event.

    Returns:
        True  -> Event created successfully.
        False -> Event already exists or creation failed.
    """

    if not event_id:
        event_id = f"{event_type}:{reference}"

    existing = query_db(
        "SELECT id FROM paystack_webhook_events WHERE event_id = %s",
        (event_id,),
        one=True,
    )

    if existing:
        return False

    sql = """
        INSERT INTO paystack_webhook_events (
            event_id,
            reference,
            event_type,
            owner_id,
            received_at,
            status,
            payload_json
        )
        VALUES (
            %s,
            %s,
            %s,
            %s,
            %s,
            'received',
            %s
        )
    """

    params = (
        event_id,
        reference,
        event_type,
        owner_id,
        utc_now(),
        payload_json,
    )

    try:
        execute_db(sql, params)
        return True

    except Exception as exc:
        print(f"Failed to create Paystack webhook event: {exc}")
        return False


def mark_webhook_event_processed(event_id, status="processed"):
    """
    Mark a webhook event as processed.
    """

    execute_db(
        """
        UPDATE paystack_webhook_events
        SET processed_at = %s,
            status = %s
        WHERE event_id = %s
        """,
        (
            utc_now(),
            status,
            event_id,
        ),
    )


def get_webhook_event(event_id):
    """
    Retrieve a webhook event by its event ID.
    """

    sql = """
        SELECT *
        FROM paystack_webhook_events
        WHERE event_id = %s
    """

    return query_db(
        sql,
        (event_id,),
        one=True,
    )
def get_pending_webhook_events(limit=DEFAULT_PENDING_LIMIT):
    """
    Retrieve webhook events waiting to be processed.

    Parameters:
        limit (int): Maximum number of events to return.

    Returns:
        list: Pending webhook events ordered by oldest first.
    """

    sql = """
        SELECT *
        FROM paystack_webhook_events
        WHERE status = 'received'
        ORDER BY received_at ASC
        LIMIT %s
    """

    return query_db(sql, (limit,))


def get_webhook_events_by_owner(owner_id, limit=None):
    """
    Retrieve webhook events for a specific owner.
    """

    sql = """
        SELECT *
        FROM paystack_webhook_events
        WHERE owner_id = %s
        ORDER BY received_at DESC
    """

    params = [owner_id]

    if limit is not None:
        sql += "\nLIMIT %s"
        params.append(limit)

    return query_db(sql, tuple(params))