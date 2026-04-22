import sys

job = sys.argv[1]

if job == "daily":
    send_day_before_reminders()

elif job == "monthly":
    send_declined_work_reminders()

elif job == "yearly":
    yearly_reminders()
    