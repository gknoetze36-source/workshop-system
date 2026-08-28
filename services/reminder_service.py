from datetime import datetime, timedelta
from constants.message_categories import (
    BOOKING_CONFIRMATION, BOOKING_REMINDER, VEHICLE_READY, SERVICE_FOLLOWUP,
)

from database import execute_db, utc_now
from helpers.common import boolish, fetch_all, fetch_one, scope_clause
from helpers.dates import human_date, month_end, parse_date, sast_now, sast_today
from services.financial_service import can_send_messages, track_message_usage
from services.message_templates import (
    build_booking_message,
    build_booking_confirmation_message,
    build_appointment_reminder_message,
    build_vehicle_ready_message,
)
from services.messaging_service import send_cheapest_message
from services.messaging_provider import active_messaging_account, send_provider_message
from services.communication_service import log_communication, update_reminder_status
from services.messaging_service import can_send_outbound





def send_booking_confirmation(reference):
    booking = fetch_one(
        """
        SELECT
            b.*,
            f.name AS location_name,
            f.slug AS location_slug,
            br.name AS location_name,
            br.slug AS location_slug
        FROM bookings b
        LEFT JOIN locations f ON f.id = b.location_id
        LEFT JOIN locations br ON br.id = b.location_id
        WHERE b.booking_reference=%s
        """,
        (reference,),
    )
    if not booking:
        return False, "booking not found"
    subject, body = build_booking_confirmation_message(booking)
    return send_cheapest_message(booking, subject, body, category=BOOKING_CONFIRMATION)


def send_vehicle_ready_notification(booking, actor_user_id=None):
    subject, body = build_vehicle_ready_message(booking)
    return send_cheapest_message(booking, subject, body, actor_user_id=actor_user_id, category=VEHICLE_READY)


def send_booking_reminders(days_ahead=1, label=None):
    target_date = (sast_now() + timedelta(days=int(days_ahead or 0))).strftime("%Y-%m-%d")
    label = label or ("Today" if int(days_ahead or 0) == 0 else "Tomorrow")
    reminder_kind = "booking_same_day" if int(days_ahead or 0) == 0 else "booking_day_before"
    bookings = fetch_all(
        """
        SELECT
            b.*,
            f.name AS location_name,
            f.slug AS location_slug,
            br.name AS location_name,
            br.slug AS location_slug,
            br.contact_email AS location_contact_email,
            br.contact_phone AS location_contact_phone
        FROM bookings b
        LEFT JOIN locations f ON f.id = b.location_id
        LEFT JOIN locations br ON br.id = b.location_id
        WHERE b.scheduled_date=%s
          AND b.status IN ('Pending', 'Confirmed', 'In Progress')
          AND COALESCE(b.reminder_opt_in, TRUE)=TRUE
          AND COALESCE(b.phone, '') <> ''
          AND COALESCE(f.active, TRUE)=TRUE
          AND COALESCE(br.active, TRUE)=TRUE
        ORDER BY b.scheduled_date ASC, b.id ASC
        """,
        (target_date,),
    )
    sent = 0
    for booking in bookings:
        subject, body = build_appointment_reminder_message(booking, label)
        reminder = ensure_reminder_campaign(booking, reminder_kind, target_date, subject, body)
        if reminder.get("status") == "Sent":
            continue
        success, channel = send_cheapest_message(booking, subject, body, reminder=reminder, category=BOOKING_REMINDER)
        if success:
            update_reminder_status(reminder["id"], "Sent", channel, count_as_send=True)
            sent += 1
    return sent


def ensure_reminder_campaign(booking, reminder_kind, scheduled_for, subject, body, campaign_round=1):
    reminder = fetch_one(
        """
        SELECT *
        FROM reminder_campaigns
        WHERE booking_id=%s AND reminder_kind=%s AND campaign_round=%s
        """,
        (booking["id"], reminder_kind, campaign_round),
    )
    if reminder:
        return reminder
    execute_db(
        """
        INSERT INTO reminder_campaigns (
            booking_id, location_id, reminder_kind, due_date,
            campaign_round, scheduled_for, status, message_subject,
            message_body, send_count, created_at, updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, 'Pending', %s, %s, 0, %s, %s)
        """,
        (
            booking["id"],
            # location_id was passed twice here -- a leftover of the
            # franchise -> location migration, which made the statement supply
            # 13 values for 12 columns so every insert raised.
            booking["location_id"],
            reminder_kind,
            booking.get("scheduled_date") or scheduled_for,
            campaign_round,
            scheduled_for,
            subject,
            body,
            utc_now(),
            utc_now(),
        ),
    )
    return fetch_one(
        """
        SELECT *
        FROM reminder_campaigns
        WHERE booking_id=%s AND reminder_kind=%s AND campaign_round=%s
        """,
        (booking["id"], reminder_kind, campaign_round),
    )

def generate_due_reminders(user_scope=None, as_of=None, force=False):
    as_of = as_of or sast_now()
    clause = "1=1"
    args = []
    if user_scope:
        clause, args = scope_clause(user_scope)

    bookings = fetch_all(
        f"""
        SELECT
            b.*,
            f.name AS location_name,
            f.slug AS location_slug,
            br.name AS location_name,
            br.slug AS location_slug,
            br.contact_email AS location_contact_email,
            br.contact_phone AS location_contact_phone
        FROM bookings b
        LEFT JOIN locations f ON f.id = b.location_id
        LEFT JOIN locations br ON br.id = b.location_id
        WHERE {clause}
          AND b.status IN ('Done', 'Collected')
          AND COALESCE(b.reminder_opt_in, TRUE) = TRUE
          AND (
              (b.service_level IN ('Major', 'Minor') AND COALESCE(b.service_due_date, '') <> '')
              OR COALESCE(b.work_to_be_done, '') <> ''
          )
        ORDER BY b.service_due_date ASC
        """,
        tuple(args),
    )

    created = 0
    for booking in bookings:
        due_date = parse_date(booking.get("service_due_date"))
        scheduled_date = parse_date(booking.get("scheduled_date"))

        reminder_types = []
        if booking.get("service_level") in {"Major", "Minor"} and due_date:
            reminder_types.append((f"{booking['service_level'].lower()}_service", due_date, [month_end(due_date), month_end(month_end(due_date) + timedelta(days=1))]))
        if booking.get("work_to_be_done"):
            work_due = due_date or scheduled_date or as_of
            reminder_types.append(("work_to_be_done", work_due, [month_end(work_due), month_end(month_end(work_due) + timedelta(days=1))]))

        for reminder_kind, due_for_message, campaign_dates in reminder_types:
            for round_number, campaign_date in enumerate(campaign_dates, start=1):
                if not campaign_date:
                    continue
                window_end = campaign_date + timedelta(days=31)
                if not force and not (campaign_date <= as_of <= window_end):
                    continue

                existing = fetch_one(
                    """
                    SELECT id
                    FROM reminder_campaigns
                    WHERE booking_id=%s AND reminder_kind=%s AND campaign_round=%s
                    """,
                    (booking["id"], reminder_kind, round_number),
                )
                if existing:
                    continue

                due_value = due_for_message.strftime("%Y-%m-%d") if hasattr(due_for_message, "strftime") else (booking.get("service_due_date") or booking.get("scheduled_date"))
                subject, body = build_booking_message(booking, {"due_date": due_value})
                execute_db(
                    """
                    INSERT INTO reminder_campaigns (
                        booking_id, location_id, reminder_kind, due_date,
                        campaign_round, scheduled_for, status, message_subject,
                        message_body, send_count, created_at, updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, 'Pending', %s, %s, 0, %s, %s)
                    """,
                    (
                        booking["id"],
                        # location_id was passed twice -- franchise-era leftover.
                        booking["location_id"],
                        reminder_kind,
                        due_value,
                        round_number,
                        campaign_date.strftime("%Y-%m-%d"),
                        subject,
                        body,
                        utc_now(),
                        utc_now(),
                    ),
                )
                created += 1

    return created


def auto_send_reminder(reminder, actor_user=None):
    booking = fetch_one(
        """
        SELECT
            b.*,
            f.name AS location_name,
            f.slug AS location_slug,
            br.name AS location_name,
            br.slug AS location_slug,
            br.contact_email AS location_contact_email,
            br.contact_phone AS location_contact_phone
        FROM bookings b
        LEFT JOIN locations f ON f.id = b.location_id
        LEFT JOIN locations br ON br.id = b.location_id
        WHERE b.id=%s
        """,
        (reminder["booking_id"],),
    )
    if not booking:
        return False, "Booking not found."

    subject, body = build_booking_message(booking, reminder)
    if not can_send_outbound(booking, subject, body, category=BOOKING_REMINDER):
        return False, "Outbound messaging is disabled for this client account."
    if not boolish(booking.get("whatsapp_opt_in", 0)):
        return False, "WhatsApp opt-in is disabled for this customer."
    try:
        account = active_messaging_account(booking)
        if account and booking.get("phone"):
            send_provider_message(booking["phone"], body, booking, account=account)
            log_communication(booking, reminder, "whatsapp", booking["phone"], subject, body, f"sent:{account.get('provider')}", actor_user["id"] if actor_user else None)
            track_message_usage(booking.get("location_id"))
            update_reminder_status(reminder["id"], "Sent", "whatsapp", count_as_send=True)
            return True, f"WhatsApp message sent by {account.get('provider')}."
    except Exception as exc:
        log_communication(booking, reminder, "whatsapp", booking.get("phone", ""), subject, body, f"failed: {exc}", actor_user["id"] if actor_user else None)
        return False, str(exc)
    return False, "No active messaging account is configured for this workshop."



def send_missed_booking_followups():
    today = sast_today()
    bookings = fetch_all(
        """
        SELECT
            b.*,
            f.name AS location_name,
            f.slug AS location_slug,
            br.name AS location_name,
            br.slug AS location_slug,
            br.contact_email AS location_contact_email,
            br.contact_phone AS location_contact_phone
        FROM bookings b
        LEFT JOIN locations f ON f.id = b.location_id
        LEFT JOIN locations br ON br.id = b.location_id
        WHERE b.scheduled_date < %s
          AND b.status IN ('Pending', 'Confirmed', 'In Progress')
          AND COALESCE(b.reminder_opt_in, TRUE) = TRUE
          AND COALESCE(b.phone, '') <> ''
          AND COALESCE(b.missed_followup_count, 0) < 2
          AND COALESCE(f.active, TRUE) = TRUE
          AND COALESCE(br.active, TRUE) = TRUE
        ORDER BY b.scheduled_date ASC
        """,
        (today,),
    )
    sent = 0
    for booking in bookings:
        recent_reply = fetch_one(
            """
            SELECT id
            FROM chatbot_messages
            WHERE location_id=%s
              AND customer_phone=%s
              AND direction='inbound'
              AND created_at > COALESCE(%s, '1900-01-01T00:00:00')
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (booking["location_id"], booking["phone"], booking.get("last_missed_followup_at")),
        )
        if recent_reply:
            execute_db("UPDATE bookings SET last_customer_reply_at=%s, updated_at=%s WHERE id=%s AND location_id=%s", (utc_now(), utc_now(), booking["id"], booking["location_id"]))
            continue
        subject = f"{booking.get('location_name')}: missed booking follow-up"
        body = (
            f"Hello {booking.get('first_name') or 'Customer'}, we missed you for your booking on "
            f"{human_date(booking.get('scheduled_date'))}. Reply here if you would like us to reschedule."
        )
        success, channel = send_cheapest_message(booking, subject, body, category=SERVICE_FOLLOWUP)
        if success:
            execute_db(
                "UPDATE bookings SET missed_followup_count=%s, last_missed_followup_at=%s, updated_at=%s WHERE id=%s AND location_id=%s",
                (int(booking.get("missed_followup_count") or 0) + 1, utc_now(), utc_now(), booking["id"], booking["location_id"]),
            )
            sent += 1
    return sent



