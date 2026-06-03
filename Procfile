web: sh -c "gunicorn app:app --bind 0.0.0.0:${PORT:-8080} --workers ${WEB_CONCURRENCY:-1} --threads ${GUNICORN_THREADS:-2} --timeout ${GUNICORN_TIMEOUT:-60}"
worker: python automation_worker.py
scheduler: python scheduler.py
billing: python cron_jobs.py billing
