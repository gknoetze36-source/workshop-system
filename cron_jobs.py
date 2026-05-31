from automation_engine import process_due_jobs
from database import initialize_database
from platform_helpers import close_billing_period, expire_due_subscriptions, fetch_all, month_end
from platform_messaging import (
    auto_send_reminder,
    build_declined_work_reminder_message,
    fetch_reminders_for_user,
    generate_due_reminders,
    send_booking_reminders,
    send_cheapest_message,
    send_inquiry_followups,
    send_missed_booking_followups,
    sast_now,
    sast_today,
)
import sys


def prepare_database():
    state = initialize_database()
    print(f"Database ready: {state['backend']}")


# ---------------- DAY BEFORE ---------------- #

def send_day_before_reminders():
    total = send_booking_reminders(days_ahead=1, label="Tomorrow")
    print(f"Day-before booking reminders sent: {total}")


def send_same_day_reminders():
    total = send_booking_reminders(days_ahead=0, label="Today")
    print(f"Same-day booking reminders sent: {total}")


# ---------------- DECLINED WORK ---------------- #

def send_declined_work_reminders():
    today = sast_now()
    if today.date() != month_end(today).date():
        print("Declined reminders skipped: not month-end")
        return

    bookings = fetch_all(
        """
        SELECT
            b.*,
            f.name AS franchise_name,
            f.slug AS franchise_slug,
            br.name AS branch_name,
            br.slug AS branch_slug,
            br.contact_email AS branch_contact_email,
            br.contact_phone AS branch_contact_phone
        FROM bookings b
        LEFT JOIN franchises f ON f.id = b.franchise_id
        LEFT JOIN branches br ON br.id = b.branch_id
        WHERE b.quote_declined = 'Yes'
          AND COALESCE(b.work_to_be_done, '') <> ''
          AND b.status <> 'Declined'
          AND COALESCE(b.reminder_opt_in, TRUE) = TRUE
          AND COALESCE(b.phone, '') <> ''
          AND COALESCE(f.active, TRUE) = TRUE
          AND COALESCE(br.active, TRUE) = TRUE
        ORDER BY b.scheduled_date ASC, b.id ASC
        """
    )

    sent = 0
    for b in bookings:
        subject, body = build_declined_work_reminder_message(b)
        success, _channel = send_cheapest_message(b, subject, body)
        if success:
            sent += 1

    print(f"Declined reminders sent: {sent}")


# ---------------- YEARLY ---------------- #

def yearly_reminders():
    created = generate_due_reminders()
    sent = 0
    for reminder in fetch_reminders_for_user({"role": "super_admin"}):
        if reminder.get("status") == "Pending" and str(reminder.get("scheduled_for") or "") <= sast_today():
            success, _ = auto_send_reminder(reminder)
            if success:
                sent += 1
    print(f"Running yearly reminders... created={created} sent={sent}")


def send_missed_booking_jobs():
    total = send_missed_booking_followups()
    print(f"Missed-booking follow-ups sent: {total}")


def send_inquiry_followup_jobs():
    total = send_inquiry_followups()
    print(f"Inquiry follow-ups sent: {total}")


def process_automation_jobs():
    total = process_due_jobs()
    print(f"Automation jobs processed: {total}")


def subscription_check_jobs():
    expire_due_subscriptions()
    print("Subscription status checked")


def billing_close_jobs():
    total = close_billing_period()
    print(f"Billing records closed: {total}")


# ---------------- ENTRY ---------------- #

if __name__ == "__main__":
    job = sys.argv[1] if len(sys.argv) > 1 else None
    prepare_database()

    if job == "daily":
        subscription_check_jobs()
        send_day_before_reminders()
        send_same_day_reminders()

    elif job == "monthly":
        send_declined_work_reminders()
        yearly_reminders()

    elif job == "same-day":
        send_same_day_reminders()

    elif job == "day-before":
        send_day_before_reminders()

    elif job == "yearly":
        yearly_reminders()

    elif job == "missed":
        send_missed_booking_jobs()

    elif job == "inquiry":
        send_inquiry_followup_jobs()

    elif job == "automation":
        process_automation_jobs()

    elif job == "subscriptions":
        subscription_check_jobs()

    elif job == "billing":
        billing_close_jobs()

    else:
        print("No valid job provided")
