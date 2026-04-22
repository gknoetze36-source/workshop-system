from datetime import datetime, timedelta

from flask import has_request_context, url_for

from database import classify_service_level, execute_db, iso_date, parse_any_date, query_db, utc_now

ROLE_LABELS = {
    "reception": "Reception",
    "franchise_admin": "Franchise Admin",
    "super_admin": "Platform Super Admin",
}

PLAN_DEFINITIONS = {
    "basic": {"label": "Basic", "branch_limit": 1, "user_limit": 2, "automation_enabled": 0, "chatbot_enabled": 0, "reporting_enabled": 0, "custom_integrations_enabled": 0, "priority_support_enabled": 0},
    "growth": {"label": "Growth", "branch_limit": 5, "user_limit": 10, "automation_enabled": 1, "chatbot_enabled": 1, "reporting_enabled": 1, "custom_integrations_enabled": 0, "priority_support_enabled": 0},
    "premium": {"label": "Premium", "branch_limit": 999999, "user_limit": 999999, "automation_enabled": 1, "chatbot_enabled": 1, "reporting_enabled": 1, "custom_integrations_enabled": 1, "priority_support_enabled": 1},
}

STATUS_OPTIONS = ["Pending", "Confirmed", "In Progress", "Done", "Collected", "Declined"]
CONTACT_OPTIONS = ["WhatsApp", "SMS", "Email", "Phone Call"]
DONE_STATUSES = {"Done", "Collected"}


def fetch_one(query, args=()):
    return query_db(query, args=args, one=True)


def fetch_all(query, args=()):
    return query_db(query, args=args) or []


def utc_today():
    return datetime.utcnow().strftime("%Y-%m-%d")


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
        [31, 29 if year % 4 == 0 and (year % 100 != 0 or year % 400 == 0) else 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31][
            month - 1
        ],
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


def role_label(value):
    return ROLE_LABELS.get(value, value or "Unknown")


def plan_label(value):
    return PLAN_DEFINITIONS.get((value or "").lower(), {}).get("label", value or "Unknown")


def boolish(value):
    return str(value).lower() in {"1", "true", "yes", "on"}


def scope_clause(user, alias="b"):
    if user["role"] == "super_admin":
        return "1=1", []
    if user["role"] == "franchise_admin":
        return f"{alias}.franchise_id = %s", [user["franchise_id"]]
    return f"{alias}.branch_id = %s", [user["branch_id"]]


def user_scope_clause(user, alias="u"):
    if user["role"] == "super_admin":
        return "1=1", []
    return f"{alias}.franchise_id = %s", [user["franchise_id"]]


def visible_franchises(user=None, include_inactive=False):
    clauses = []
    args = []
    if not include_inactive:
        clauses.append("active = 1")
    if user and user["role"] != "super_admin":
        clauses.append("id = %s")
        args.append(user["franchise_id"])
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    return fetch_all(f"SELECT * FROM franchises {where} ORDER BY name", tuple(args))


def franchise_counts(franchise_id):
    branch_total = fetch_one("SELECT COUNT(*) AS total FROM branches WHERE franchise_id=%s AND COALESCE(active, 1)=1", (franchise_id,))
    user_total = fetch_one("SELECT COUNT(*) AS total FROM users WHERE franchise_id=%s AND COALESCE(active, 1)=1", (franchise_id,))
    return {
        "branches": int((branch_total or {}).get("total") or 0),
        "users": int((user_total or {}).get("total") or 0),
    }


def can_add_branch(franchise):
    counts = franchise_counts(franchise["id"])
    limit = int(franchise.get("branch_limit") or 0)
    return limit <= 0 or counts["branches"] < limit


def can_add_user(franchise):
    counts = franchise_counts(franchise["id"])
    limit = int(franchise.get("user_limit") or 0)
    return limit <= 0 or counts["users"] < limit


def fetch_service_prices(user):
    if user["role"] == "super_admin":
        return fetch_all(
            """
            SELECT sp.*, f.name AS franchise_name, b.name AS branch_name
            FROM service_prices sp
            LEFT JOIN franchises f ON f.id = sp.franchise_id
            LEFT JOIN branches b ON b.id = sp.branch_id
            ORDER BY f.name, b.name, sp.service_name
            """
        )
    return fetch_all(
        """
        SELECT sp.*, f.name AS franchise_name, b.name AS branch_name
        FROM service_prices sp
        LEFT JOIN franchises f ON f.id = sp.franchise_id
        LEFT JOIN branches b ON b.id = sp.branch_id
        WHERE sp.franchise_id=%s
        ORDER BY b.name, sp.service_name
        """,
        (user["franchise_id"],),
    )


def find_service_price(franchise_id, branch_id, service_name):
    service_name = (service_name or "").strip()
    if not service_name:
        return None
    return (
        fetch_one(
            "SELECT * FROM service_prices WHERE franchise_id=%s AND branch_id=%s AND lower(service_name)=lower(%s) AND COALESCE(active,1)=1",
            (franchise_id, branch_id, service_name),
        )
        or fetch_one(
            "SELECT * FROM service_prices WHERE franchise_id=%s AND branch_id IS NULL AND lower(service_name)=lower(%s) AND COALESCE(active,1)=1",
            (franchise_id, service_name),
        )
    )
def get_franchise_report(franchise_id):
    return fetch_all("""
        SELECT b.name, COUNT(*) as bookings, SUM(price) as revenue
        FROM bookings
        JOIN branches b ON b.id = bookings.branch_id
        WHERE franchise_id=%s
        GROUP BY b.name
    """, (franchise_id,))

def get_service_profit(franchise_id):
    return fetch_all("""
        SELECT service, SUM(price) as revenue
        FROM bookings
        WHERE franchise_id=%s
        GROUP BY service
    """, (franchise_id,))

def generate_invoice(franchise_id):
    usage = fetch_one("""
        SELECT chatbot_messages_used 
        FROM franchises WHERE id=%s
    """, (franchise_id,))

    used = usage["chatbot_messages_used"]
    extra = max(0, used - 200)

    total = 2000 + (extra * 1)

    execute_db("""
        INSERT INTO invoices (franchise_id, month, total_messages, extra_messages, amount)
        VALUES (%s, %s, %s, %s, %s)
    """, (franchise_id, "2026-04", used, extra, total))

    return total

def monthly_usage_summary(user=None):
    clauses = []
    args = []
    if user and user["role"] != "super_admin":
        clauses.append("cum.franchise_id=%s")
        args.append(user["franchise_id"])
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    return fetch_all(
        """
        SELECT cum.*, f.name AS franchise_name, f.plan_code, f.monthly_base_price, f.monthly_message_limit, f.overage_price_per_message, f.active
        FROM chatbot_usage_monthly cum
        LEFT JOIN franchises f ON f.id = cum.franchise_id
        """
        + where
        + " ORDER BY cum.usage_month DESC, f.name",
        tuple(args),
    )


def plan_features(franchise):
    plan = PLAN_DEFINITIONS.get((franchise.get("plan_code") or "basic").lower(), PLAN_DEFINITIONS["basic"])
    features = [
        f"{plan['label']} plan",
        "1 branch only" if plan["branch_limit"] == 1 else ("Up to 5 branches" if plan["branch_limit"] == 5 else "Unlimited branches"),
        "2 users" if plan["user_limit"] == 2 else ("Up to 10 users" if plan["user_limit"] == 10 else "Unlimited users"),
        "Automations enabled" if plan["automation_enabled"] else "No automations",
        "Chatbot enabled" if plan["chatbot_enabled"] else "No chatbot automation",
        "Reporting dashboard" if plan["reporting_enabled"] else "Basic dashboard only",
        "Priority support" if plan["priority_support_enabled"] else "Standard support",
        "Custom integrations" if plan["custom_integrations_enabled"] else "No custom integrations",
    ]
    return features


def daily_usage_summary(user=None):
    clauses = []
    args = []
    if user and user["role"] != "super_admin":
        clauses.append("cud.franchise_id=%s")
        args.append(user["franchise_id"])
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    return fetch_all(
        """
        SELECT cud.*, f.name AS franchise_name
        FROM chatbot_usage_daily cud
        LEFT JOIN franchises f ON f.id = cud.franchise_id
        """
        + where
        + " ORDER BY cud.usage_date DESC, f.name",
        tuple(args),
    )


def visible_branches(user=None, franchise_id=None, include_inactive=False, public_only=False):
    clauses = []
    args = []
    if not include_inactive:
        clauses.append("b.active = 1")
    if public_only:
        clauses.append("b.public_booking_enabled = 1")
    if user:
        if user["role"] == "reception":
            clauses.append("b.id = %s")
            args.append(user["branch_id"])
        elif user["role"] == "franchise_admin":
            clauses.append("b.franchise_id = %s")
            args.append(user["franchise_id"])
    if franchise_id:
        clauses.append("b.franchise_id = %s")
        args.append(franchise_id)

    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    return fetch_all(
        """
        SELECT
            b.*,
            f.name AS franchise_name,
            f.slug AS franchise_slug
        FROM branches b
        LEFT JOIN franchises f ON f.id = b.franchise_id
        """
        + where
        + " ORDER BY f.name, b.name",
        tuple(args),
    )


def branch_for_public_booking(franchise_slug, branch_slug):
    return fetch_one(
        """
        SELECT
            b.*,
            f.name AS franchise_name,
            f.slug AS franchise_slug
        FROM branches b
        JOIN franchises f ON f.id = b.franchise_id
        WHERE f.slug=%s AND b.slug=%s AND b.active=1 AND b.public_booking_enabled=1
        """,
        (franchise_slug, branch_slug),
    )


def branch_by_id(branch_id):
    return fetch_one(
        """
        SELECT
            b.*,
            f.name AS franchise_name,
            f.slug AS franchise_slug
        FROM branches b
        LEFT JOIN franchises f ON f.id = b.franchise_id
        WHERE b.id=%s
        """,
        (branch_id,),
    )


def generate_booking_reference(scheduled_date):
    prefix = f"BK-{(scheduled_date or utc_today()).replace('-', '')}"
    existing = fetch_all(
        "SELECT booking_reference FROM bookings WHERE booking_reference LIKE %s ORDER BY booking_reference DESC",
        (f"{prefix}-%",),
    )
    number = len(existing) + 1
    while True:
        reference = f"{prefix}-{number:04d}"
        if not fetch_one("SELECT id FROM bookings WHERE booking_reference=%s", (reference,)):
            return reference
        number += 1


def public_booking_url(branch):
    if has_request_context():
        return url_for(
            "public_branch_booking",
            franchise_slug=branch["franchise_slug"],
            branch_slug=branch["slug"],
            _external=True,
        )

    base = ""
    path = f"/book/{branch['franchise_slug']}/{branch['slug']}"
    return f"{base}{path}" if base else path


def fetch_visible_bookings(user, filters=None):
    filters = filters or {}
    clause, args = scope_clause(user)
    where = [clause]

    search = (filters.get("search") or "").strip().lower()
    if search:
        where.append(
            """
            (
                lower(COALESCE(b.booking_reference, '')) LIKE %s OR
                lower(COALESCE(b.first_name, '')) LIKE %s OR
                lower(COALESCE(b.surname, '')) LIKE %s OR
                lower(COALESCE(b.phone, '')) LIKE %s OR
                lower(COALESCE(b.make, '')) LIKE %s OR
                lower(COALESCE(b.model, '')) LIKE %s OR
                lower(COALESCE(b.service, '')) LIKE %s
            )
            """
        )
        args.extend([f"%{search}%"] * 7)

    if filters.get("status"):
        where.append("b.status = %s")
        args.append(filters["status"])

    if filters.get("scheduled_date"):
        where.append("b.scheduled_date = %s")
        args.append(filters["scheduled_date"])

    if filters.get("branch_id"):
        where.append("b.branch_id = %s")
        args.append(filters["branch_id"])

    if filters.get("franchise_id"):
        where.append("b.franchise_id = %s")
        args.append(filters["franchise_id"])

    return fetch_all(
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
        WHERE
        """
        + " AND ".join(where)
        + " ORDER BY b.scheduled_date ASC, b.created_at DESC",
        tuple(args),
    )


def booking_in_scope(booking, user):
    if not booking:
        return False
    if user["role"] == "super_admin":
        return True
    if user["role"] == "franchise_admin":
        return booking.get("franchise_id") == user.get("franchise_id")
    return booking.get("branch_id") == user.get("branch_id")


def fetch_booking_for_user(reference, user):
    booking = fetch_one(
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
        WHERE b.booking_reference=%s
        """,
        (reference,),
    )
    return booking if booking_in_scope(booking, user) else None


def selected_branch_for_user(user, branch_id=None):
    if user and user["role"] == "reception":
        return branch_by_id(user["branch_id"])

    if branch_id:
        branch = branch_by_id(branch_id)
        if branch and (user["role"] == "super_admin" or branch["franchise_id"] == user["franchise_id"]):
            return branch
        return None

    visible = visible_branches(user=user)
    return visible[0] if visible else None


def available_roles_for_creator(user):
    if user["role"] == "super_admin":
        return ["reception", "franchise_admin", "super_admin"]
    return ["reception", "franchise_admin"]


def insert_booking(branch, form_data, source, status):
    scheduled_date = iso_date(form_data.get("scheduled_date") or form_data.get("date")) or utc_today()
    service = (form_data.get("service") or "").strip()
    service_level = classify_service_level(service)
    completed_at = scheduled_date if status in DONE_STATUSES else ""
    service_due_date = compute_service_due_date(service_level, completed_at)
    booking_reference = generate_booking_reference(scheduled_date)
    reminder_opt_in = 1 if boolish(form_data.get("reminder_opt_in", "true")) else 0
    now = utc_now()

    execute_db(
        """
        INSERT INTO bookings (
            booking_reference, franchise_id, branch_id, company, branch,
            first_name, surname, customer_email, phone, preferred_contact_method,
            make, model, vehicle_year, fuel_type, vehicle_vin, service, service_level,
            current_mileage, scheduled_date, date, status, service_due_date, work_to_be_done,
            public_notes, internal_notes, source, quote_declined, contacted, reminder_opt_in,
            completed_at, created_at, updated_at
        )
        VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s
        )
        """,
        (
            booking_reference,
            branch["franchise_id"],
            branch["id"],
            branch["franchise_name"],
            branch["name"],
            (form_data.get("first_name") or "").strip(),
            (form_data.get("surname") or "").strip(),
            (form_data.get("customer_email") or form_data.get("email") or "").strip(),
            (form_data.get("phone") or "").strip(),
            (form_data.get("preferred_contact_method") or "WhatsApp").strip(),
            (form_data.get("make") or "").strip(),
            (form_data.get("model") or "").strip(),
            (form_data.get("vehicle_year") or "").strip(),
            (form_data.get("fuel_type") or "").strip(),
            (form_data.get("vehicle_vin") or "").strip(),
            service,
            service_level,
            (form_data.get("current_mileage") or "").strip(),
            scheduled_date,
            scheduled_date,
            status,
            service_due_date,
            (form_data.get("work_to_be_done") or form_data.get("work") or "").strip(),
            (form_data.get("public_notes") or "").strip(),
            (form_data.get("internal_notes") or "").strip(),
            source,
            (form_data.get("quote_declined") or "No").strip(),
            "No",
            reminder_opt_in,
            completed_at or None,
            now,
            now,
        ),
    )
    return booking_reference
