import time
from datetime import datetime, timedelta

from cron_jobs import send_day_before_reminders, send_declined_work_reminders, send_inquiry_followup_jobs, send_missed_booking_jobs, yearly_reminders


# ---------------- DAY BEFORE REMINDERS ---------------- #

def run_scheduler():
    print("Scheduler started...")
    last_daily = None
    last_evening = None
    last_yearly = None

    while True:
        now = datetime.now()
        minute_bucket = now.strftime("%Y-%m-%d %H:%M")

        if now.minute % 5 == 0:
            send_inquiry_followup_jobs()

        # Run every day at 08:00
        if now.hour == 8 and last_daily != minute_bucket[:10]:
            send_day_before_reminders()
            last_daily = minute_bucket[:10]

        # Run every day at 18:00
        if now.hour == 18 and last_evening != minute_bucket[:10]:
            send_declined_work_reminders()
            send_missed_booking_jobs()
            last_evening = minute_bucket[:10]

        # Run every day at 19:00
        if now.hour == 19 and last_yearly != minute_bucket[:10]:
            yearly_reminders()
            last_yearly = minute_bucket[:10]

        time.sleep(300)


if __name__ == "__main__":
    run_scheduler()
    
