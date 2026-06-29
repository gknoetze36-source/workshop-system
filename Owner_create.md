# Owner Creation Guide

This document explains how to create an "Owner" in the Workshop SaaS platform, which maps to the Franchise entity in the database.

## Overview
In the Workshop system:
- **Owner** = Franchise (business entity that owns the subscription)
- **Location** = Branch (physical business locations)
- **User** = User (staff/employees)

## Step-by-Step Owner Creation Process

### 1. HTTP Request Handler (app.py:1549-1607)
When a POST request is made to `/manage/franchises`:

**Line 1550-1551**: Requires super_admin role
```python
@roles_required("super_admin")
```

**Line 1553**: Get franchise name from form
```python
name = (request.form.get("name") or "").strip()
```

**Line 1554**: Check if franchise name is unique (case-insensitive)
```python
if name and not fetch_one("SELECT id FROM franchises WHERE lower(name)=lower(%s)", (name,)):
```

**Lines 1555-1557**: Get plan code and plan definition
```python
plan_code = (request.form.get("plan_code") or "basic").lower()
plan = PLAN_DEFINITIONS.get(plan_code, PLAN_DEFINITIONS["basic"])
```

**Line 1557**: Set subscription dates
```python
subscription_start = utc_today()
subscription_end = (datetime.utcnow() + timedelta(days=30)).strftime("%Y-%m-%d")
```

**Lines 1559-1595**: Execute database INSERT for franchises table
```python
execute_db(
    """
    INSERT INTO franchises (
        name, slug, contact_email, contact_phone, notes, industry, subscription_status,
        subscription_start, subscription_end, setup_fee, plan_code, branch_limit, user_limit,
        automation_enabled, chatbot_enabled, reporting_enabled, custom_integrations_enabled,
        priority_support_enabled, monthly_base_price, monthly_message_limit, overage_price_per_message,
        billing_day, public_base_url, inbound_webhook_token, active, created_at, updated_at
    ) VALUES (%s, %s, %s, %s, %s, %s, 'active', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """,
    (
        name,                                           # Line 1570
        __import__('database').slugify(name),          # Line 1571 - creates URL-friendly slug
        (request.form.get("contact_email") or "").strip(), # Line 1572
        (request.form.get("contact_phone") or "").strip(), # Line 1573
        (request.form.get("notes") or "").strip(),     # Line 1574
        (request.form.get("industry") or "workshop").strip().lower(), # Line 1575
        subscription_start,                            # Line 1576
        subscription_end,                              # Line 1577
        float(request.form.get("setup_fee") or 0),     # Line 1578
        plan_code,                                     # Line 1579
        plan["branch_limit"],                          # Line 1580
        plan["user_limit"],                            # Line 1581
        db_bool(plan["automation_enabled"]),           # Line 1582
        db_bool(plan["chatbot_enabled"]),              # Line 1583
        db_bool(plan["reporting_enabled"]),            # Line 1584
        db_bool(plan["custom_integrations_enabled"]),  # Line 1585
        db_bool(plan["priority_support_enabled"]),     # Line 1586
        float(request.form.get("monthly_base_price") or 0), # Line 1587
        int(request.form.get("monthly_message_limit") or 2000), # Line 1588
        float(request.form.get("overage_price_per_message") or 0.5), # Line 1589
        (request.form.get("public_base_url") or "").strip(), # Line 1590
        (request.form.get("inbound_webhook_token") or "").strip(), # Line 1591
        db_bool(True),                                 # Line 1592 - active = True
        utc_now(),                                     # Line 1593 - created_at
        utc_now()                                      # Line 1594 - updated_at
    ),
)
```

**Line 1596**: Get the created franchise ID
```python
created = fetch_one("SELECT id FROM franchises WHERE slug=%s", (__import__('database').slugify(name),))
```

### 2. Business Provisioning (platform_helpers.py:363-447)
After creating the franchise record, `provision_business` is called (line 1599 in app.py):

**Line 363**: Function signature
```python
def provision_business(franchise_id, answers=None):
```

**Lines 364-366**: Validate franchise exists
```python
franchise = fetch_one("SELECT * FROM franchises WHERE id=%s", (franchise_id,))
if not franchise:
    return {"ok": False, "error": "business not found"}
```

**Lines 368-370**: Process answers and get industry/plan
```python
answers = answers or {}
industry = (answers.get("industry") or franchise.get("industry") or "workshop").strip().lower()
plan_code = (answers.get("plan") or franchise.get("plan_code") or "basic").strip().lower()
plan = PLAN_DEFINITIONS.get(plan_code, PLAN_DEFINITIONS["basic"])
```

**Line 372**: Get industry template
```python
template = fetch_one("SELECT * FROM industry_templates WHERE industry=%s AND active=TRUE", (industry,))
```

**Line 373**: Determine message limit
```python
message_limit = int(answers.get("monthly_message_limit") or (template or {}).get("default_message_limit") or franchise.get("monthly_message_limit") or 2000)
```

**Lines 375-397**: Update franchises table with provisioning details
```python
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
        industry,                                    # Line 385
        plan_code,                                   # Line 386
        plan["branch_limit"],                        # Line 387
        plan["user_limit"],                          # Line 388
        db_bool(plan["automation_enabled"]),         # Line 389
        db_bool(plan["chatbot_enabled"]),            # Line 390
        db_bool(plan["reporting_enabled"]),          # Line 391
        db_bool(plan["custom_integrations_enabled"]), # Line 392
        db_bool(plan["priority_support_enabled"]),   # Line 393
        message_limit,                               # Line 394
        utc_now(),                                   # Line 395
        franchise_id                                 # Line 396
    ),
)
```

**Lines 399-406**: Create/update feature flags for this franchise
```python
for key in ("automation_enabled", "chatbot_enabled", "reporting_enabled", "custom_integrations_enabled", "priority_support_enabled"):
    existing = fetch_one("SELECT id FROM feature_flags WHERE franchise_id=%s AND feature_key=%s", (franchise_id, key))
    enabled = db_bool(plan.get(key, 0))
    if existing:
        execute_db("UPDATE feature_flags SET enabled=%s, updated_at=%s WHERE id=%s", (enabled, utc_now(), existing["id"]))
    else:
        execute_db("INSERT INTO feature_flags (franchise_id, feature_key, enabled, created_at, updated_at) VALUES (%s, %s, %s, %s, %s)", (franchise_id, key, enabled, utc_now(), utc_now()))
```

**Line 408**: Get first branch for this franchise (if any)
```python
branch = fetch_one("SELECT * FROM branches WHERE franchise_id=%s ORDER BY id LIMIT 1", (franchise_id,))
```

**Lines 409-410**: Create default services for the industry
```python
for service_name in DEFAULT_SERVICES_BY_INDUSTRY.get(industry, ["Consultation", "Booking", "Follow-up"]):
    ensure_service(franchise_id, branch.get("id") if branch else None, service_name)
```

**Lines 412-430**: Create automation rules if automation is enabled
```python
assigned_rules = 0
if boolish(plan.get("automation_enabled", 0)):
    templates = fetch_all("SELECT * FROM automation_templates WHERE industry=%s AND active=TRUE", (industry,))
    for item in templates:
        existing = fetch_one("SELECT id FROM automation_rules WHERE franchise_id=%s AND template_id=%s", (franchise_id, item["id"]))
        action_json = '{"type":"send_message","job_type":"send_message"}' if item.get("event_type") == "booking.created" else '{"type":"log","job_type":"automation_log"}'
        if existing:
            execute_db("UPDATE automation_rules SET active=%s, delay_minutes=%s, updated_at=%s WHERE id=%s", (db_bool(True), item.get("default_delay_minutes") or 0, utc_now(), existing["id"]))
        else:
            execute_db(
                """
                INSERT INTO automation_rules (
                    franchise_id, template_id, name, event_type, conditions_json, action_json,
                    delay_minutes, active, created_at, updated_at
                ) VALUES (%s, %s, %s, %s, '{}', %s, %s, %s, %s, %s)
                """,
                (franchise_id, item["id"], item["name"], item["event_type"], action_json, item.get("default_delay_minutes") or 0, db_bool(True), utc_now(), utc_now()),
            )
            assigned_rules += 1
```

**Lines 432-439**: Update onboarding sessions
```python
session_row = fetch_one("SELECT id FROM onboarding_sessions WHERE franchise_id=%s ORDER BY id DESC LIMIT 1", (franchise_id,))
if not session_row:
    execute_db(
        "INSERT INTO onboarding_sessions (franchise_id, industry, selected_plan, status, current_step, started_at, completed_at, created_at, updated_at) VALUES (%s, %s, %s, 'completed', 'provisioned', %s, %s, %s, %s)",
        (franchise_id, industry, plan_code, utc_now(), utc_now(), utc_now(), utc_now(), utc_now()),
    )
else:
    execute_db("UPDATE onboarding_sessions SET industry=%s, selected_plan=%s, status='completed', current_step='provisioned', completed_at=%s, updated_at=%s WHERE id=%s", (industry, plan_code, utc_now(), utc_now(), session_row["id"]))
```

**Lines 441-445**: Update onboarding state
```python
state = fetch_one("SELECT id FROM onboarding_state WHERE franchise_id=%s", (franchise_id,))
if state:
    execute_db("UPDATE onboarding_state SET setup_progress=100, services_created=%s, automations_enabled=%s, go_live_ready=%s, updated_at=%s WHERE id=%s", (db_bool(True), db_bool(plan.get("automation_enabled", 0)), db_bool(True), utc_now(), state["id"]))
else:
    execute_db("INSERT INTO onboarding_state (franchise_id, setup_progress, services_created, automations_enabled, go_live_ready, created_at, updated_at) VALUES (%s, 100, %s, %s, %s, %s, %s)", (franchise_id, db_bool(True), db_bool(plan.get("automation_enabled", 0)), db_bool(True), utc_now(), utc_now()))
```

**Line 447**: Return success
```python
return {"ok": True, "industry": industry, "plan": plan_code, "message_limit": message_limit, "automation_rules_created": assigned_rules}
```

### 3. Helper Functions Used

#### ensure_service (platform_helpers.py:973-992)
Called to create default services for the Owner's locations:

**Lines 973-974**: Function signature and service name cleaning
```python
def ensure_service(franchise_id, branch_id, service_name):
    service_name = (service_name or "").strip()
    if not service_name:
        return None
```

**Lines 977-982**: Check if service already exists
```python
service = fetch_one(
    "SELECT id FROM services WHERE franchise_id=%s AND COALESCE(branch_id, 0)=COALESCE(%s, 0) AND lower(name)=lower(%s) ORDER BY id DESC LIMIT 1",
    (franchise_id, branch_id, service_name),
)
if service:
    return service["id"]
```

**Line 983**: Get service price if it exists
```python
price = find_service_price(franchise_id, branch_id, service_name)
```

**Lines 984-990**: Insert new service
```python
execute_db(
    """
    INSERT INTO services (franchise_id, branch_id, name, category, price_amount, active, created_at, updated_at)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """,
    (franchise_id, branch_id, service_name, (price or {}).get("service_category"), float((price or {}).get("price_amount") or 0), db_bool(True), utc_now(), utc_now()),
)
```

**Lines 991-992**: Return the service ID
```python
row = fetch_one("SELECT id FROM services WHERE franchise_id=%s AND lower(name)=lower(%s) ORDER BY id DESC LIMIT 1", (franchise_id, service_name))
return row["id"] if row else None
```

## Database Schema for Owner (Franchise)

When an Owner is created, a record is inserted into the `franchises` table with these key fields:

**Core Identification** (from _create_tables function in database.py:292-323):
- `id`: Primary key (SERIAL for PostgreSQL, INTEGER AUTOINCREMENT for SQLite)
- `name`: Business name (TEXT NOT NULL)
- `slug`: URL-friendly name (TEXT)
- `contact_email`: Email for billing/payments (TEXT)
- `contact_phone`: Business phone number (TEXT)
- `notes`: Additional notes (TEXT)
- `industry`: Business type (TEXT DEFAULT 'workshop')
- `subscription_status`: Subscription state (TEXT DEFAULT 'active')
- `subscription_start`: Start date (TEXT)
- `subscription_end`: End date (TEXT)
- `setup_fee`: Initial setup fee (REAL DEFAULT 0)
- `public_base_url`: Public URL for the business (TEXT)
- `inbound_webhook_token`: Security token for webhooks (TEXT)
- `plan_code`: Subscription plan (TEXT DEFAULT 'basic')
- `branch_limit`: Maximum branches allowed (INTEGER DEFAULT 1)
- `user_limit`: Maximum users allowed (INTEGER DEFAULT 2)
- `automation_enabled`: Automation features (BOOLEAN/INTEGER DEFAULT FALSE)
- `chatbot_enabled`: Chatbot features (BOOLEAN/INTEGER DEFAULT FALSE)
- `reporting_enabled`: Reporting features (BOOLEAN/INTEGER DEFAULT FALSE)
- `custom_integrations_enabled`: Custom integrations (BOOLEAN/INTEGER DEFAULT FALSE)
- `priority_support_enabled`: Priority support (BOOLEAN/INTEGER DEFAULT FALSE)
- `monthly_base_price`: Base monthly price (REAL DEFAULT 0)
- `monthly_message_limit`: Included messages per month (INTEGER DEFAULT 2000)
- `messages_used`: Messages used this month (INTEGER DEFAULT 0)
- `overage_price_per_message`: Price per extra message (REAL DEFAULT 0.5)
- `billing_day`: Day of month for billing (TEXT)
- `active`: Whether franchise is active (BOOLEAN/INTEGER DEFAULT TRUE)
- `created_at`: Creation timestamp (TEXT)
- `updated_at`: Last update timestamp (TEXT)

## Related Tables Created/Updated

When an Owner is created, the following tables may be affected:

1. **services**: Default services for the industry are created via `ensure_service`
2. **feature_flags**: Individual feature flags are set based on the plan
3. **onboarding_sessions**: Tracks onboarding progress
4. **onboarding_state**: Detailed onboarding state tracking
5. **automation_rules**: Default automation rules are created if automation is enabled
6. **branches**: While not created during franchise creation, the relationship is established (branches have franchise_id foreign key)
7. **users**: While not created during franchise creation, the relationship is established (users have franchise_id foreign key)
8. **chatbot_usage_monthly**: Initialized when first billing period closes
9. **usage_daily**: Tracks daily message usage
10. **billing_records**: Created when billing periods are closed

## Key Mappings to Your Requested Terminology

| Your Term | System Term | Database Table | Key Fields |
|-----------|-------------|----------------|------------|
| **Owner** | Franchise | `franchises` | `monthly_base_price`, `messages_used`, `overage_price_per_message` (billing info) |
| **Location** | Branch | `branches` | `location`, `contact_email`, `contact_phone` (business info for customers) |
| **User** | User | `users` | `role`, `franchise_id`, `branch_id` (staff roles) |

## How Message Usage is Tracked for Owner Billing

1. **Daily tracking**: `track_message_usage()` in platform_helpers.py (lines 225-270)
   - Updates `usage_daily` table for today's count
   - Updates `chatbot_usage_monthly` table for month-to-date count
   - Increments `franchises.messages_used` counter

2. **Monthly billing**: `close_billing_period()` in platform_helpers.py (lines 273-312)
   - Called daily via cron/scheduler
   - Calculates overage: `max(0, message_count - monthly_message_limit)`
   - Calculates overage cost: `extra_messages * overage_price_per_message`
   - Calculates total due: `monthly_base_price + overage_cost`
   - Creates/updates `billing_records` record
   - Resets counters for next month via `reset_monthly_usage()` in billing_scheduler.py

## New Pricing Structure (Workshop System Pricing Calculator)

As of the latest update, the Workshop System uses a dynamic pricing calculator based on the business's actual revenue. This replaces the fixed monthly subscription model with a value-based pricing approach.

### Pricing Formula Implementation

The new pricing structure is implemented as a Python function that calculates fees based on the workshop's actual business metrics:

```python
def calculate_workshop_pricing(
    average_invoice_value,
    average_cars_per_month,
    percentage=0.02,
    actual_messages_used=0
):
    """
    Workshop System Pricing Calculator

    Formula:
    Total Revenue = Average Invoice Value × Average Cars
    Monthly Fee = Total Revenue × Percentage
    Setup Fee = Monthly Fee × 2
    Free Messages = Average Cars × 3
    Extra Messages = Used Messages - Free Messages
    Extra Message Cost = Extra Messages × R0.10
    """

    # Step 1: Calculate monthly customer value
    total_monthly_revenue = average_invoice_value * average_cars_per_month

    # Step 2: Calculate monthly system fee
    monthly_fee = total_monthly_revenue * percentage

    # Step 3: Calculate setup fee
    setup_fee = monthly_fee * 2

    # Step 4: Calculate free WhatsApp messages
    free_messages = average_cars_per_month * 3

    # Step 5: Calculate extra messages
    extra_messages = max(actual_messages_used - free_messages, 0)

    # Step 6: Calculate extra message cost
    extra_message_cost = extra_messages * 0.10

    return {
        "average_invoice_value": average_invoice_value,
        "average_cars_per_month": average_cars_per_month,
        "total_monthly_revenue": total_monthly_revenue,
        "percentage_charged": f"{percentage * 100}%",
        "monthly_system_fee": monthly_fee,
        "setup_fee": setup_fee,
        "free_messages": free_messages,
        "messages_used": actual_messages_used,
        "extra_messages": extra_messages,
        "extra_message_cost": extra_message_cost,
        "first_month_total": setup_fee + monthly_fee + extra_message_cost
    }


# ==========================
# Example Workshop
# ==========================

workshop = calculate_workshop_pricing(
    average_invoice_value=2500,
    average_cars_per_month=300,
    percentage=0.02,
    actual_messages_used=5000
)


# Display Invoice

print("====== WORKSHOP SYSTEM INVOICE ======")

for item, value in workshop.items():
    print(f"{item}: R{value:,.2f}" if isinstance(value, (int, float)) else f"{item}: {value}")
```

### How This Affects Owner Creation

When creating an Owner (Franchise), the following fields in the `franchises` table are now calculated dynamically rather than set as fixed values:

1. **`monthly_base_price`**: Set to the calculated `monthly_system_fee` from the pricing calculator
2. **`setup_fee`**: Set to the calculated `setup_fee` (2× monthly fee)
3. **`monthly_message_limit`**: Set to the calculated `free_messages` (Average Cars × 3)
4. **`overage_price_per_message`**: Fixed at R0.10 per extra message

The pricing calculator is typically invoked during:
- Initial franchise creation/setup
- Monthly billing cycles (via `close_billing_period()`)
- When franchise owners update their business metrics through the dashboard

### Example Calculation

For a workshop with:
- Average invoice value: R2,500
- Average cars per month: 300
- Percentage charged: 2%
- Actual messages used: 5,000

The pricing calculator would produce:
- Total monthly revenue: R750,000 (2,500 × 300)
- Monthly system fee: R15,000 (750,000 × 0.02)
- Setup fee: R30,000 (15,000 × 2)
- Free messages: 900 (300 × 3)
- Extra messages: 4,100 (5,000 - 900)
- Extra message cost: R410 (4,100 × 0.10)
- First month total: R45,410 (30,000 + 15,000 + 410)

## Files Modified During Owner Creation

1. **app.py**: HTTP endpoint handler (`/manage/franchises` POST)
2. **platform_helpers.py**: 
   - `provision_business()` function (main provisioning logic)
   - `ensure_service()` function (creates default services)
   - Various helper functions (`fetch_one`, `fetch_all`, `execute_db`, `utc_now`, etc.)
3. **database.py**: Database connection and query execution
4. **Database tables modified**:
   - `franchises` (INSERT)
   - `services` (INSERT via ensure_service)
   - `feature_flags` (INSERT/UPDATE)
   - `onboarding_sessions` (INSERT/UPDATE)
   - `onboarding_state` (INSERT/UPDATE)
   - `automation_rules` (INSERT if automation enabled)
   - Plus various tracking tables as usage occurs

This completes the line-by-line explanation of how to create an Owner in the Workshop SaaS platform.