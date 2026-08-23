from datetime import datetime
import re

from .query import execute_db


def utc_now():
    return datetime.utcnow().replace(microsecond=0).isoformat()


def slugify(value):
    text = re.sub(r"[^a-z0-9]+", "-", str(value or "").strip().lower())
    return text.strip("-") or "item"


def parse_any_date(value):
    text = str(value or "").strip()
    if not text:
        return None

    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y", "%m-%d-%Y", "%m-%d-%y", "%m/%d/%Y %H:%M:%S"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def iso_date(value):
    parsed = parse_any_date(value)
    return parsed.strftime("%Y-%m-%d") if parsed else ""


def classify_service_level(service_name):
    text = str(service_name or "").lower()
    if "major" in text:
        return "Major"
    if "minor" in text:
        return "Minor"
    return "General"
