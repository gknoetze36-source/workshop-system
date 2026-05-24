web: sh -c "gunicorn app:app --bind 0.0.0.0:${PORT:-8080}"
worker: python automation_worker.py
scheduler: python scheduler.py
billing: python cron_jobs.py billing
