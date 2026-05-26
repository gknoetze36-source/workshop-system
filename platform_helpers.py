from datetime import datetime, timedelta
import os

from flask import has_request_context, url_for

from database import classify_service_level, execute_db, iso_date, parse_any_date, query_db, transaction, utc_now
from validators.phone_validator import normalize_phone

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

DEFAULT_SERVICES_BY_INDUSTRY = {
    "workshop": ["Service", "Repairs", "Inspection"],
    "salon": ["Consultation", "Hair Appointment", "Treatment"],
    "dentist": ["Consultation", "Checkup", "Follow-up"],
    "clinic": ["Consultation", "Follow-up", "Procedure"],
    "hotel": ["Room Booking", "Check-in", "Guest Request"],
    "consultant": ["Consultation", "Strategy Session", "Follow-up"],
    "gym": ["Class Booking", "Personal Training", "Assessment"],
    "cleaning": ["Once-off Cleaning", "Recurring Cleaning", "Deep Clean"],
    "repair": ["Repair Booking", "Quote", "Collection"],
}

STATUS_OPTIONS = ["Pending", "Confirmed", "In Progress", "Done", "Collected", "Declined"]
CONTACT_OPTIONS = ["WhatsApp", "SMS", "Email", "Phone Call"]
DONE_STATUSES = {"Done", "Collected"}
INQUIRY_STATES = ["NEW_INQUIRY", "ENGAGED", "BOOKING_PENDING", "BOOKED", "LOST"]


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


def refresh_subscription_status(franchise):
    if not franchise:
        return None
    subscription_end = parse_date(franchise.get("subscription_end"))
    current_status = (franchise.get("subscription_status") or "active").lower()
    if subscription_end and subscription_end.date() < datetime.utcnow().date() and current_status not in {"inactive", "cancelled"}:
        execute_db(
            "UPDATE franchises SET subscription_status='inactive', updated_at=%s WHERE id=%s",
            (utc_now(), franchise["id"]),
        )
        franchise = dict(franchise)
        franchise["subscription_status"] = "inactive"
    return franchise


def subscription_status(franchise):
    franchise = refresh_subscription_status(franchise)
    if not franchise:
        return "inactive"
    if not boolish(franchise.get("active", 1)):
        return "inactive"
    return (franchise.get("subscription_status") or "active").lower()


def subscription_is_active(franchise):
    return subscription_status(franchise) in {"active", "trialing"}


def feature_enabled(franchise, feature_key):
    if not franchise:
        return False
    flag = fetch_one(
        "SELECT enabled FROM feature_flags WHERE franchise_id=%s AND feature_key=%s",
        (franchise["id"], feature_key),
    )
    if flag is not None:
        return boolish(flag.get("enabled", 0))
    plan = PLAN_DEFINITIONS.get((franchise.get("plan_code") or "basic").lower(), PLAN_DEFINITIONS["basic"])
    return boolish(plan.get(feature_key, franchise.get(feature_key, 0)))


def can_use_paid_feature(franchise, feature_key=None):
    if not subscription_is_active(franchise):
        return False
    return feature_enabled(franchise, feature_key) if feature_key else True


def can_create_booking(franchise):
    return can_use_paid_feature(franchise)


def can_run_automation(franchise):
    return can_use_paid_feature(franchise, "automation_enabled")


def can_send_messages(franchise):
    return can_use_paid_feature(franchise)


def track_message_usage(franchise_id, count=1):
    if not franchise_id or count <= 0:
        return
    today = utc_today()
    month_key = today[:7]
    franchise = fetch_one("SELECT * FROM franchises WHERE id=%s", (franchise_id,))
    if not franchise:
        return

    limit = int(franchise.get("monthly_message_limit") or 2000)
    overage_price = float(franchise.get("overage_price_per_message") or 0.5)
    base_price = float(franchise.get("monthly_base_price") or 0)

    daily = fetch_one("SELECT * FROM usage_daily WHERE franchise_id=%s AND usage_date=%s", (franchise_id, today))
    if daily:
        messages_used = int(daily.get("messages_used") or 0) + count
        extra_messages = max(messages_used - limit, 0)
        execute_db(
            "UPDATE usage_daily SET messages_used=%s, extra_messages=%s, extra_cost=%s, updated_at=%s WHERE id=%s",
            (messages_used, extra_messages, extra_messages * overage_price, utc_now(), daily["id"]),
        )
    else:
        execute_db(
            "INSERT INTO usage_daily (franchise_id, usage_date, messages_used, extra_messages, extra_cost, created_at, updated_at) VALUES (%s, %s, %s, 0, 0, %s, %s)",
            (franchise_id, today, count, utc_now(), utc_now()),
        )

    monthly = fetch_one("SELECT * FROM chatbot_usage_monthly WHERE franchise_id=%s AND usage_month=%s", (franchise_id, month_key))
    if monthly:
        message_count = int(monthly.get("message_count") or 0) + count
        extra_messages = max(message_count - limit, 0)
        overage_cost = extra_messages * overage_price
        execute_db(
            "UPDATE chatbot_usage_monthly SET message_count=%s, message_limit=%s, extra_messages=%s, base_price=%s, overage_price=%s, overage_cost=%s, total_due=%s, updated_at=%s WHERE id=%s",
            (message_count, limit, extra_messages, base_price, overage_price, overage_cost, base_price + overage_cost, utc_now(), monthly["id"]),
        )
    else:
        execute_db(
            "INSERT INTO chatbot_usage_monthly (franchise_id, usage_month, message_count, message_limit, extra_messages, base_price, overage_price, overage_cost, total_due, created_at, updated_at) VALUES (%s, %s, %s, %s, 0, %s, %s, 0, %s, %s, %s)",
            (franchise_id, month_key, count, limit, base_price, overage_price, base_price, utc_now(), utc_now()),
        )

    execute_db(
        "UPDATE franchises SET messages_used=COALESCE(messages_used, 0) + %s, updated_at=%s WHERE id=%s",
        (count, utc_now(), franchise_id),
    )


def close_billing_period(usage_month=None, franchise_id=None):
    usage_month = (usage_month or utc_today()[:7]).strip()
    clauses = ["cum.usage_month=%s"]
    args = [usage_month]
    if franchise_id:
        clauses.append("cum.franchise_id=%s")
        args.append(franchise_id)
    rows = fetch_all(
        """
        SELECT cum.*, f.monthly_base_price, f.monthly_message_limit, f.overage_price_per_message
        FROM chatbot_usage_monthly cum
        LEFT JOIN franchises f ON f.id = cum.franchise_id
        WHERE
        """
        + " AND ".join(clauses),
        tuple(args),
    )
    with transaction():
        closed = 0
        for row in rows:
            limit = int(row.get("message_limit") or row.get("monthly_message_limit") or 2000)
            overage_price = float(row.get("overage_price") or row.get("overage_price_per_message") or 0.5)
            base_price = float(row.get("base_price") or row.get("monthly_base_price") or 0)
            extra = max(int(row.get("message_count") or 0) - limit, 0)
            usage_amount = extra * overage_price
            total_due = base_price + usage_amount
            execute_db(
                "UPDATE chatbot_usage_monthly SET message_limit=%s, extra_messages=%s, base_price=%s, overage_price=%s, overage_cost=%s, total_due=%s, updated_at=%s WHERE id=%s",
                (limit, extra, base_price, overage_price, usage_amount, total_due, utc_now(), row["id"]),
            )
            existing = fetch_one("SELECT id FROM billing_records WHERE franchise_id=%s AND billing_period=%s", (row["franchise_id"], usage_month))
            if existing:
                execute_db("UPDATE billing_records SET amount=%s, base_amount=%s, usage_amount=%s, updated_at=%s WHERE id=%s", (total_due, base_price, usage_amount, utc_now(), existing["id"]))
            else:
                execute_db(
                    "INSERT INTO billing_records (franchise_id, amount, base_amount, usage_amount, status, billing_period, created_at, updated_at) VALUES (%s, %s, %s, %s, 'unpaid', %s, %s, %s)",
                    (row["franchise_id"], total_due, base_price, usage_amount, usage_month, utc_now(), utc_now()),
                )
            closed += 1
        return closed


def mark_billing_paid(franchise_id, billing_period, payment_reference=""):
    subscription_start = utc_today()
    subscription_end = (datetime.utcnow() + timedelta(days=30)).strftime("%Y-%m-%d")
    with transaction():
        execute_db(
            """
            UPDATE franchises
            SET subscription_status='active', subscription_start=%s, subscription_end=%s,
                messages_used=0, updated_at=%s
            WHERE id=%s
            """,
            (subscription_start, subscription_end, utc_now(), franchise_id),
        )
        execute_db(
            "UPDATE chatbot_usage_monthly SET payment_status='Paid', paid_at=%s, payment_reference=%s, updated_at=%s WHERE franchise_id=%s AND usage_month=%s",
            (utc_now(), payment_reference, utc_now(), franchise_id, billing_period),
        )
        execute_db(
            "UPDATE billing_records SET status='paid', payment_reference_id=%s, paid_at=%s, updated_at=%s WHERE franchise_id=%s AND billing_period=%s",
            (payment_reference, utc_now(), utc_now(), franchise_id, billing_period),
        )


def expire_due_subscriptions():
    today = utc_today()
    execute_db(
        "UPDATE franchises SET subscription_status='inactive', updated_at=%s WHERE active=1 AND subscription_end IS NOT NULL AND subscription_end<>'' AND subscription_end < %s AND subscription_status NOT IN ('inactive','cancelled')",
        (utc_now(), today),
    )


def create_payment_link(billing_id):
    billing = fetch_one("SELECT br.*, f.name AS franchise_name FROM billing_records br LEFT JOIN franchises f ON f.id=br.franchise_id WHERE br.id=%s", (billing_id,))
    if not billing:
        return None
    base_url = os.environ.get("PUBLIC_BASE_URL", "").rstrip("/")
    reference = f"billing-{billing['id']}-{billing['billing_period']}".replace(" ", "-")
    callback_url = f"{base_url}/manage/franchises" if base_url else ""
    email = os.environ.get("BILLING_EMAIL") or os.environ.get("ADMIN_EMAIL") or "billing@example.com"
    metadata = {"franchise_id": billing["franchise_id"], "billing_period": billing["billing_period"], "billing_record_id": billing["id"]}
    from services.paystack import initialize_transaction

    result = initialize_transaction(email, billing.get("amount") or 0, reference, callback_url=callback_url, metadata=metadata)
    payment_link = ((result.get("data") or {}).get("authorization_url")) or callback_url
    execute_db("UPDATE billing_records SET payment_link=%s, updated_at=%s WHERE id=%s", (payment_link, utc_now(), billing_id))
    return payment_link


def provision_business(franchise_id, answers=None):
    franchise = fetch_one("SELECT * FROM franchises WHERE id=%s", (franchise_id,))
    if not franchise:
        return {"ok": False, "error": "business not found"}

    answers = answers or {}
    industry = (answers.get("industry") or franchise.get("industry") or "workshop").strip().lower()
    plan_code = (answers.get("plan") or franchise.get("plan_code") or "basic").strip().lower()
    plan = PLAN_DEFINITIONS.get(plan_code, PLAN_DEFINITIONS["basic"])
    template = fetch_one("SELECT * FROM industry_templates WHERE industry=%s AND active=1", (industry,))
    message_limit = int(answers.get("monthly_message_limit") or (template or {}).get("default_message_limit") or franchise.get("monthly_message_limit") or 2000)

    execute_db(
        """
        UPDATE franchises
        SET industry=%s, plan_code=%s, branch_limit=%s, user_limit=%s,
            automation_enabled=%s, chatbot_enabled=%s, reporting_enabled=%s,
            custom_integrations_enabled=%s, priority_support_enabled=%s,
            monthly_message_limit=%s, updated_at=%s
        WHERE id=%s
        """,
        (
            industry,
            plan_code,
            plan["branch_limit"],
            plan["user_limit"],
            plan["automation_enabled"],
            plan["chatbot_enabled"],
            plan["reporting_enabled"],
            plan["custom_integrations_enabled"],
            plan["priority_support_enabled"],
            message_limit,
            utc_now(),
            franchise_id,
        ),
    )

    for key in ("automation_enabled", "chatbot_enabled", "reporting_enabled", "custom_integrations_enabled", "priority_support_enabled"):
        existing = fetch_one("SELECT id FROM feature_flags WHERE franchise_id=%s AND feature_key=%s", (franchise_id, key))
        enabled = 1 if boolish(plan.get(key, 0)) else 0
        if existing:
            execute_db("UPDATE feature_flags SET enabled=%s, updated_at=%s WHERE id=%s", (enabled, utc_now(), existing["id"]))
        else:
            execute_db("INSERT INTO feature_flags (franchise_id, feature_key, enabled, created_at, updated_at) VALUES (%s, %s, %s, %s, %s)", (franchise_id, key, enabled, utc_now(), utc_now()))

    branch = fetch_one("SELECT * FROM branches WHERE franchise_id=%s ORDER BY id LIMIT 1", (franchise_id,))
    for service_name in DEFAULT_SERVICES_BY_INDUSTRY.get(industry, ["Consultation", "Booking", "Follow-up"]):
        ensure_service(franchise_id, branch.get("id") if branch else None, service_name)

    assigned_rules = 0
    if boolish(plan.get("automation_enabled", 0)):
        templates = fetch_all("SELECT * FROM automation_templates WHERE industry=%s AND active=1", (industry,))
        for item in templates:
            existing = fetch_one("SELECT id FROM automation_rules WHERE franchise_id=%s AND template_id=%s", (franchise_id, item["id"]))
            action_json = '{"type":"send_message","job_type":"send_message"}' if item.get("event_type") == "booking.created" else '{"type":"log","job_type":"automation_log"}'
            if existing:
                execute_db("UPDATE automation_rules SET active=1, delay_minutes=%s, updated_at=%s WHERE id=%s", (item.get("default_delay_minutes") or 0, utc_now(), existing["id"]))
            else:
                execute_db(
                    """
                    INSERT INTO automation_rules (
                        franchise_id, template_id, name, event_type, conditions_json, action_json,
                        delay_minutes, active, created_at, updated_at
                    ) VALUES (%s, %s, %s, %s, '{}', %s, %s, 1, %s, %s)
                    """,
                    (franchise_id, item["id"], item["name"], item["event_type"], action_json, item.get("default_delay_minutes") or 0, utc_now(), utc_now()),
                )
                assigned_rules += 1

    session_row = fetch_one("SELECT id FROM onboarding_sessions WHERE franchise_id=%s ORDER BY id DESC LIMIT 1", (franchise_id,))
    if not session_row:
        execute_db(
            "INSERT INTO onboarding_sessions (franchise_id, industry, selected_plan, status, current_step, started_at, completed_at, created_at, updated_at) VALUES (%s, %s, %s, 'completed', 'provisioned', %s, %s, %s, %s)",
            (franchise_id, industry, plan_code, utc_now(), utc_now(), utc_now(), utc_now()),
        )
    else:
        execute_db("UPDATE onboarding_sessions SET industry=%s, selected_plan=%s, status='completed', current_step='provisioned', completed_at=%s, updated_at=%s WHERE id=%s", (industry, plan_code, utc_now(), utc_now(), session_row["id"]))

    state = fetch_one("SELECT id FROM onboarding_state WHERE franchise_id=%s", (franchise_id,))
    if state:
        execute_db("UPDATE onboarding_state SET setup_progress=100, services_created=1, automations_enabled=%s, go_live_ready=1, updated_at=%s WHERE id=%s", (1 if boolish(plan.get("automation_enabled", 0)) else 0, utc_now(), state["id"]))
    else:
        execute_db("INSERT INTO onboarding_state (franchise_id, setup_progress, services_created, automations_enabled, go_live_ready, created_at, updated_at) VALUES (%s, 100, 1, %s, 1, %s, %s)", (franchise_id, 1 if boolish(plan.get("automation_enabled", 0)) else 0, utc_now(), utc_now()))

    return {"ok": True, "industry": industry, "plan": plan_code, "message_limit": message_limit, "automation_rules_created": assigned_rules}


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


def find_active_inquiry(franchise_id, branch_id, phone="", email=""):
    phone = (phone or "").strip()
    email = (email or "").strip().lower()
    if phone:
        inquiry = fetch_one(
            """
            SELECT *
            FROM booking_inquiries
            WHERE franchise_id=%s
              AND branch_id=%s
              AND customer_phone=%s
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (franchise_id, branch_id, phone),
        )
        if inquiry:
            return inquiry
    if email:
        return fetch_one(
            """
            SELECT *
            FROM booking_inquiries
            WHERE franchise_id=%s
              AND branch_id=%s
              AND lower(COALESCE(customer_email, ''))=lower(%s)
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (franchise_id, branch_id, email),
        )
    return None


def fetch_inquiries_for_user(user, limit=30):
    clause, args = scope_clause(user, alias="bi")
    return fetch_all(
        f"""
        SELECT
            bi.*,
            b.booking_reference,
            br.name AS branch_name,
            f.name AS franchise_name
        FROM booking_inquiries bi
        LEFT JOIN bookings b ON b.id = bi.booking_id
        LEFT JOIN branches br ON br.id = bi.branch_id
        LEFT JOIN franchises f ON f.id = bi.franchise_id
        WHERE {clause}
        ORDER BY bi.updated_at DESC, bi.created_at DESC
        LIMIT %s
        """,
        tuple(args + [limit]),
    )


def inquiry_metrics(user):
    clause, args = scope_clause(user, alias="bi")
    row = fetch_one(
        f"""
        SELECT
            COUNT(*) AS total_inquiries,
            SUM(CASE WHEN bi.user_state='NEW_INQUIRY' THEN 1 ELSE 0 END) AS new_inquiries,
            SUM(CASE WHEN bi.user_state='ENGAGED' THEN 1 ELSE 0 END) AS engaged_inquiries,
            SUM(CASE WHEN bi.user_state='BOOKING_PENDING' THEN 1 ELSE 0 END) AS booking_pending,
            SUM(CASE WHEN bi.user_state='BOOKED' THEN 1 ELSE 0 END) AS booked_inquiries,
            SUM(CASE WHEN bi.user_state='LOST' THEN 1 ELSE 0 END) AS lost_inquiries,
            SUM(COALESCE(bi.followups_sent_count, 0)) AS followups_sent,
            SUM(COALESCE(bi.replies_after_followup_count, 0)) AS replies_after_followup,
            SUM(COALESCE(bi.bookings_from_followups_count, 0)) AS bookings_from_followups
        FROM booking_inquiries bi
        WHERE {clause}
        """,
        tuple(args),
    ) or {}
    return {key: int(row.get(key) or 0) for key in [
        "total_inquiries",
        "new_inquiries",
        "engaged_inquiries",
        "booking_pending",
        "booked_inquiries",
        "lost_inquiries",
        "followups_sent",
        "replies_after_followup",
        "bookings_from_followups",
    ]}


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
    franchise = fetch_one("SELECT * FROM franchises WHERE id=%s", (branch["franchise_id"],)) if branch.get("franchise_id") else None
    base_url = (franchise or {}).get("public_base_url") or ""
    if has_request_context():
        path = url_for(
            "public_branch_booking",
            franchise_slug=branch["franchise_slug"],
            branch_slug=branch["slug"],
            _external=not bool(base_url),
        )
        return f"{base_url.rstrip('/')}{url_for('public_branch_booking', franchise_slug=branch['franchise_slug'], branch_slug=branch['slug'])}" if base_url else path

    path = f"/book/{branch['franchise_slug']}/{branch['slug']}"
    return f"{base_url.rstrip('/')}{path}" if base_url else path


def fetch_credential_audit():
    return fetch_all(
        """
        SELECT ca.*, u.full_name AS actor_name, f.name AS franchise_name
        FROM credential_audit ca
        LEFT JOIN users u ON u.id = ca.actor_user_id
        LEFT JOIN franchises f ON f.id = ca.franchise_id
        ORDER BY ca.created_at DESC
        """
    )


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
    return assert_tenant_scope(booking, user)


def assert_tenant_scope(row, user, branch_key="branch_id", franchise_key="franchise_id"):
    if not row or not user:
        return False
    if user["role"] == "super_admin":
        return True
    if user["role"] == "franchise_admin":
        return row.get(franchise_key) == user.get("franchise_id")
    return row.get(branch_key) == user.get("branch_id")


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


def upsert_customer(franchise_id, form_data):
    phone = normalize_phone(form_data.get("phone"))
    email = (form_data.get("customer_email") or form_data.get("email") or "").strip().lower()
    first_name = (form_data.get("first_name") or "").strip()
    surname = (form_data.get("surname") or "").strip()
    full_name = " ".join(part for part in [first_name, surname] if part).strip() or (form_data.get("customer_name") or "").strip()
    customer = None
    if phone:
        customer = fetch_one("SELECT * FROM customers WHERE franchise_id=%s AND phone=%s ORDER BY id DESC LIMIT 1", (franchise_id, phone))
    if not customer and email:
        customer = fetch_one("SELECT * FROM customers WHERE franchise_id=%s AND lower(COALESCE(email, ''))=lower(%s) ORDER BY id DESC LIMIT 1", (franchise_id, email))
    if customer:
        execute_db(
            """
            UPDATE customers
            SET first_name=COALESCE(NULLIF(%s, ''), first_name),
                surname=COALESCE(NULLIF(%s, ''), surname),
                full_name=COALESCE(NULLIF(%s, ''), full_name),
                phone=COALESCE(NULLIF(%s, ''), phone),
                email=COALESCE(NULLIF(%s, ''), email),
                accepts_whatsapp=%s,
                accepts_sms=%s,
                updated_at=%s
            WHERE id=%s
            """,
            (first_name, surname, full_name, phone, email, 1 if boolish(form_data.get("whatsapp_opt_in", "true")) else 0, 1, utc_now(), customer["id"]),
        )
        return customer["id"]
    execute_db(
        """
        INSERT INTO customers (
            franchise_id, first_name, surname, full_name, phone, email,
            accepts_whatsapp, accepts_sms, created_at, updated_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, 1, %s, %s)
        """,
        (franchise_id, first_name, surname, full_name, phone, email, 1 if boolish(form_data.get("whatsapp_opt_in", "true")) else 0, utc_now(), utc_now()),
    )
    if phone:
        row = fetch_one("SELECT id FROM customers WHERE franchise_id=%s AND phone=%s ORDER BY id DESC LIMIT 1", (franchise_id, phone))
    else:
        row = fetch_one("SELECT id FROM customers WHERE franchise_id=%s AND lower(COALESCE(email, ''))=lower(%s) ORDER BY id DESC LIMIT 1", (franchise_id, email))
    return row["id"] if row else None


def ensure_service(franchise_id, branch_id, service_name):
    service_name = (service_name or "").strip()
    if not service_name:
        return None
    service = fetch_one(
        "SELECT id FROM services WHERE franchise_id=%s AND COALESCE(branch_id, 0)=COALESCE(%s, 0) AND lower(name)=lower(%s) ORDER BY id DESC LIMIT 1",
        (franchise_id, branch_id, service_name),
    )
    if service:
        return service["id"]
    price = find_service_price(franchise_id, branch_id, service_name)
    execute_db(
        """
        INSERT INTO services (franchise_id, branch_id, name, category, price_amount, active, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, 1, %s, %s)
        """,
        (franchise_id, branch_id, service_name, (price or {}).get("service_category"), float((price or {}).get("price_amount") or 0), utc_now(), utc_now()),
    )
    row = fetch_one("SELECT id FROM services WHERE franchise_id=%s AND lower(name)=lower(%s) ORDER BY id DESC LIMIT 1", (franchise_id, service_name))
    return row["id"] if row else None


def insert_booking(branch, form_data, source, status):
    from automation_engine import emit_event

    franchise = fetch_one("SELECT * FROM franchises WHERE id=%s", (branch["franchise_id"],))
    if not can_create_booking(franchise):
        raise PermissionError("This client account is unpaid or inactive, so new bookings are disabled.")

    scheduled_date = iso_date(form_data.get("scheduled_date") or form_data.get("date")) or utc_today()
    phone = normalize_phone(form_data.get("phone"))
    service = (form_data.get("service") or "").strip()
    service_level = classify_service_level(service)
    completed_at = scheduled_date if status in DONE_STATUSES else ""
    service_due_date = compute_service_due_date(service_level, completed_at)
    booking_reference = generate_booking_reference(scheduled_date)
    now = utc_now()
    reminder_opt_in = 1 if boolish(form_data.get("reminder_opt_in", "true")) else 0
    whatsapp_opt_in = 1 if boolish(form_data.get("whatsapp_opt_in", "false")) else 0
    privacy_consent_at = now if boolish(form_data.get("privacy_consent", "false")) else None
    customer_id = upsert_customer(branch["franchise_id"], form_data)
    service_id = ensure_service(branch["franchise_id"], branch["id"], service)

    execute_db(
        """
        INSERT INTO bookings (
            booking_reference, franchise_id, branch_id, company, branch,
            customer_id, service_id,
            first_name, surname, customer_email, phone, preferred_contact_method,
            make, model, vehicle_year, fuel_type, vehicle_vin, service, service_level,
            current_mileage, scheduled_date, date, status, service_due_date, work_to_be_done,
            public_notes, internal_notes, source, quote_declined, contacted, whatsapp_opt_in, privacy_consent_at, reminder_opt_in,
            completed_at, created_at, updated_at
        )
        VALUES (
            %s, %s, %s, %s, %s,
            %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s
        )
        """,
        (
            booking_reference,
            branch["franchise_id"],
            branch["id"],
            branch["franchise_name"],
            branch["name"],
            customer_id,
            service_id,
            (form_data.get("first_name") or "").strip(),
            (form_data.get("surname") or "").strip(),
            (form_data.get("customer_email") or form_data.get("email") or "").strip(),
            phone,
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
            whatsapp_opt_in,
            privacy_consent_at,
            reminder_opt_in,
            completed_at or None,
            now,
            now,
        ),
    )
    email = (form_data.get("customer_email") or form_data.get("email") or "").strip()
    inquiry = find_active_inquiry(branch["franchise_id"], branch["id"], phone=phone, email=email)
    if inquiry:
        followup_bookings = 1 if int(inquiry.get("followups_sent_count") or 0) > 0 else 0
        execute_db(
            """
            UPDATE booking_inquiries
            SET booking_id=(SELECT id FROM bookings WHERE booking_reference=%s),
                user_state='BOOKED',
                bookings_from_followups_count=COALESCE(bookings_from_followups_count, 0) + %s,
                stop_reason='booking_created',
                closed_at=%s,
                next_followup_at=NULL,
                updated_at=%s
            WHERE id=%s
            """,
            (booking_reference, followup_bookings, now, now, inquiry["id"]),
        )
    emit_event(
        branch["franchise_id"],
        "booking.created",
        {
            "booking_reference": booking_reference,
            "branch_id": branch["id"],
            "source": source,
            "status": status,
            "scheduled_date": scheduled_date,
        },
    )
    return booking_reference
