import time

from cron_jobs import send_day_before_reminders, send_declined_work_reminders, send_inquiry_followup_jobs, send_missed_booking_jobs, send_same_day_reminders, yearly_reminders
from database import initialize_database
from platform_messaging import sast_now


# ---------------- DAY BEFORE REMINDERS ---------------- #

def run_scheduler():
    initialize_database()
    print("Scheduler started...")
    last_daily = None
    last_evening = None
    last_yearly = None

    while True:
        now = sast_now()
        minute_bucket = now.strftime("%Y-%m-%d %H:%M")

        if 7 <= now.hour < 18 and now.minute % 5 == 0:
            send_inquiry_followup_jobs()

        if now.hour == 7 and last_daily != f"{minute_bucket[:10]}-same-day":
            send_same_day_reminders()
            last_daily = f"{minute_bucket[:10]}-same-day"

        if now.hour == 8 and last_daily != f"{minute_bucket[:10]}-day-before":
            send_day_before_reminders()
            last_daily = f"{minute_bucket[:10]}-day-before"

        # Run every day at 18:00
        if now.hour == 18 and last_evening != minute_bucket[:10]:
            send_declined_work_reminders()
            send_missed_booking_jobs()
            last_evening = minute_bucket[:10]

        if now.hour == 9 and last_yearly != minute_bucket[:10]:
            yearly_reminders()
            last_yearly = minute_bucket[:10]

        time.sleep(300)


if __name__ == "__main__":
    run_scheduler()
    
