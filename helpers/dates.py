from datetime import datetime, timedelta

SAST_OFFSET_HOURS = 2

from database import parse_any_date

def utc_today():
    return datetime.utcnow().strftime("%Y-%m-%d")

def sast_now():
    return datetime.utcnow() + timedelta(hours=SAST_OFFSET_HOURS)

def sast_today():
    return sast_now().strftime("%Y-%m-%d")


def parse_date(value):
    return parse_any_date(value)


def add_months(value, months):
    if not value:
        return None

    month_index = value.month - 1 + months
    year = value.year + month_index // 12
    month = month_index % 12 + 1

    day = min(
        value.day,
        [
            31,
            29 if year % 4 == 0 and (year % 100 != 0 or year % 400 == 0) else 28,
            31,
            30,
            31,
            30,
            31,
            31,
            30,
            31,
            30,
            31,
        ][month - 1],
    )

    return value.replace(year=year, month=month, day=day)


def month_end(value):
    if not value:
        return None

    start_next_month = add_months(value.replace(day=1), 1)
    return start_next_month - timedelta(days=1)


def compute_service_due_date(service_level, completed_on):
    parsed = parse_date(completed_on)

    if not parsed or service_level not in {"Major", "Minor"}:
        return ""

    return add_months(parsed, 12).strftime("%Y-%m-%d")


def human_date(value):
    parsed = parse_date(value)
    return parsed.strftime("%d %b %Y") if parsed else (value or "")
    