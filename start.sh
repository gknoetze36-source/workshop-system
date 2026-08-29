#!/bin/sh
exec gunicorn phanta_app:app --bind "0.0.0.0:${PORT:-8080}" --forwarded-allow-ips="*"