from datetime import datetime
from automation_engine import process_due_jobs
from database import initialize_database
from platform_helpers import close_billing_period, expire_due_subscriptions, fetch_all
from platform_messaging import (
    auto_send_reminder,
    fetch_reminders_for_user,
    generate_due_reminders,
    send_booking_reminders,
    send_cheapest_message,
    send_inquiry_followups,
    send_missed_booking_followups,
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
    bookings = fetch_all("""
        SELECT id, franchise_id, branch_id, first_name, customer_email, phone 
        FROM bookings 
        WHERE quote_declined = 'Yes'
    """)

    for b in bookings:
        send_cheapest_message(b, "Pending work reminder", "Reminder: You still have pending work. Book this month?")

    print("Declined reminders sent")


# ---------------- YEARLY ---------------- #

def yearly_reminders():
    created = generate_due_reminders()
    sent = 0
    for reminder in fetch_reminders_for_user({"role": "super_admin"}):
        if reminder.get("status") == "Pending" and str(reminder.get("scheduled_for") or "") <= datetime.utcnow().strftime("%Y-%m-%d"):
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
