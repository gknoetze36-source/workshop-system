from datetime import datetime, timedelta

from database import execute_db, fetch_all, fetch_one
from helpers.dates import parse_date
from services.message_templates import _followup_message, _followup_subject
from services.messaging_service import send_cheapest_message

def _inquiry_stage_time(stage, reference_time):
    base = reference_time if isinstance(reference_time, datetime) else parse_date(reference_time) or datetime.utcnow()
    if stage == 1:
        return (base + timedelta(minutes=FOLLOWUP_DELAYS_MINUTES[1])).replace(microsecond=0)
    if stage == 2:
        return (base + timedelta(minutes=FOLLOWUP_DELAYS_MINUTES[2])).replace(microsecond=0)
    if stage == 3:
        next_day = (base + timedelta(days=1)).replace(hour=8, minute=30, second=0, microsecond=0)
        return next_day
    if stage == 4:
        return (base + timedelta(minutes=FOLLOWUP_DELAYS_MINUTES[4])).replace(hour=9, minute=30, second=0, microsecond=0)
    return None


FOLLOWUP_DELAYS_MINUTES = {
    1: 7,
    2: 90,
    4: 60 * 24 * 2,
}
SAST_OFFSET_HOURS = 2


def send_inquiry_followups(as_of=None):
    now = as_of or datetime.utcnow()
    now_iso = _iso_now(now)
    inquiries = fetch_all(
        """
        SELECT
            bi.*,
            br.name AS location_name,
            br.slug AS location_slug
        FROM booking_inquiries bi
        LEFT JOIN locations br ON br.id = bi.location_id
        WHERE bi.booking_id IS NULL
          AND bi.user_state IN ('NEW_INQUIRY', 'ENGAGED', 'BOOKING_PENDING')
          AND COALESCE(bi.declined, 0) = 0
          AND bi.next_followup_at IS NOT NULL
          AND bi.next_followup_at <= %s
        ORDER BY bi.next_followup_at ASC
        """,
        (now_iso,),
    )
    sent = 0
    for inquiry in inquiries:
        stage = int(inquiry.get("followup_stage") or 0) + 1
        if stage > 4:
            execute_db(
                "UPDATE booking_inquiries SET user_state='LOST', stop_reason='sequence_completed', closed_at=%s, next_followup_at=NULL, updated_at=%s WHERE id=%s AND location_id=%s",
                (now_iso, now_iso, inquiry["id"], inquiry["location_id"]),
            )
            continue
        existing_event = fetch_one(
            "SELECT id FROM inquiry_followup_events WHERE inquiry_id=%s AND followup_stage=%s",
            (inquiry["id"], stage),
        )
        if existing_event:
            continue
        last_interaction = _parse_timestamp(inquiry.get("last_user_interaction_at"))
        last_followup = _parse_timestamp(inquiry.get("last_followup_at")) or datetime(1900, 1, 1)
        if int(inquiry.get("followups_sent_count") or 0) > 0 and last_interaction and last_interaction > last_followup:
            continue
        location = {
            "id": inquiry["location_id"],
            "name": inquiry.get("location_name"),
            "slug": inquiry.get("location_slug"),
            "location_id": inquiry["location_id"],
            "location_slug": inquiry.get("location_slug"),
        }
        booking_stub = {
            "id": None,
            "location_id": inquiry["location_id"],
            "phone": inquiry.get("customer_phone"),
            "customer_email": inquiry.get("customer_email"),
            "whatsapp_opt_in": 1,
            "reminder_opt_in": 1,
        }
        subject = _followup_subject(inquiry, location, stage)
        body = _followup_message(inquiry, location, stage)
        success, channel = send_cheapest_message(booking_stub, subject, body)
        status = "sent" if success else f"failed:{channel}"
        execute_db(
            """
            INSERT INTO inquiry_followup_events (
                inquiry_id, followup_stage, channel, message_subject, message_body,
                status, sent_at, created_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (inquiry["id"], stage, channel, subject, body, status, now_iso if success else None, now_iso),
        )
        if not success:
            execute_db(
                "UPDATE booking_inquiries SET updated_at=%s WHERE id=%s AND location_id=%s",
                (now_iso, inquiry["id"], inquiry["location_id"]),
            )
            continue
        next_stage_time = _inquiry_stage_time(stage + 1, now)
        execute_db(
            """
            UPDATE booking_inquiries
            SET followup_stage=%s,
                last_followup_at=%s,
                next_followup_at=%s,
                followups_sent_count=COALESCE(followups_sent_count, 0) + 1,
                updated_at=%s
            WHERE id=%s AND location_id=%s
            """,
            (
                stage,
                now_iso,
                next_stage_time.isoformat() if next_stage_time and stage < 4 else None,
                now_iso,
                inquiry["id"],
                inquiry["location_id"],
            ),
        )
        if stage >= 4:
            execute_db(
                "UPDATE booking_inquiries SET user_state='LOST', stop_reason='sequence_completed', closed_at=%s, updated_at=%s WHERE id=%s AND location_id=%s",
                (now_iso, now_iso, inquiry["id"], inquiry["location_id"]),
            )
        sent += 1
    return sent
