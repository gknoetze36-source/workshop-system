from functools import wraps

from flask import Flask, abort, flash, g, redirect, render_template, request, session, url_for
from werkzeug.security import check_password_hash, generate_password_hash

from database import execute_db, initialize_database, iso_date, query_db, utc_now
from platform_helpers import (
    CONTACT_OPTIONS,
    DONE_STATUSES,
    PLAN_DEFINITIONS,
    ROLE_LABELS,
    STATUS_OPTIONS,
    available_roles_for_creator,
    boolish,
    branch_by_id,
    branch_for_public_booking,
    can_add_branch,
    can_add_user,
    daily_usage_summary,
    fetch_all,
    fetch_booking_for_user,
    fetch_one,
    fetch_service_prices,
    fetch_visible_bookings,
    find_service_price,
    franchise_counts,
    human_date,
    insert_booking,
    monthly_usage_summary,
    plan_features,
    plan_label,
    role_label,
    selected_branch_for_user,
    user_scope_clause,
    utc_today,
    visible_branches,
    visible_franchises,
)
from platform_messaging import (
    auto_send_reminder,
    build_booking_message,
    fetch_reminder,
    fetch_reminders_for_user,
    generate_due_reminders,
    log_communication,
    manual_channel_link,
    reminder_in_scope,
    send_email_message,
    send_twilio_message,
    smtp_configured,
    twilio_configured,
    update_reminder_status,
)

app = Flask(__name__)
app.secret_key = __import__("os").environ.get("SECRET_KEY", "dev-key-change-me")

DATABASE_INIT_ERROR = None
DATABASE_STATE = None
try:
    DATABASE_STATE = initialize_database()
except Exception as exc:
    DATABASE_INIT_ERROR = exc
    if __import__("os").environ.get("DATABASE_URL"):
        raise


def current_user():
    return getattr(g, "current_user", None)


def local_database_unavailable():
    return DATABASE_INIT_ERROR is not None and not __import__("os").environ.get("DATABASE_URL")


@app.before_request
def load_current_user():
    g.current_user = None
    if not session.get("user_id"):
        return
    g.current_user = fetch_one(
        """
        SELECT u.*, f.name AS franchise_name, f.slug AS franchise_slug, b.name AS branch_name, b.slug AS branch_slug
        FROM users u
        LEFT JOIN franchises f ON f.id = u.franchise_id
        LEFT JOIN branches b ON b.id = u.branch_id
        WHERE u.id=%s
        """,
        (session["user_id"],),
    )
    if not g.current_user or not boolish(g.current_user.get("active", 1)):
        session.clear()
        g.current_user = None
        return
    if g.current_user.get("must_reset_password") and request.endpoint not in {"logout", "change_password"}:
        if request.endpoint and not request.endpoint.startswith("static"):
            return redirect(url_for("change_password"))


@app.context_processor
def inject_globals():
    return {
        "current_user": current_user(),
        "role_label": role_label,
        "plan_label": plan_label,
        "human_date": human_date,
        "plan_definitions": PLAN_DEFINITIONS,
        "status_options": STATUS_OPTIONS,
        "contact_options": CONTACT_OPTIONS,
        "today_iso": utc_today(),
    }


def login_required(view):
    @wraps(view)
    def wrapped(*args, **kwargs):
        if not current_user():
            return redirect(url_for("login", next=request.path))
        return view(*args, **kwargs)

    return wrapped


def roles_required(*roles):
    def decorator(view):
        @wraps(view)
        def wrapped(*args, **kwargs):
            if not current_user():
                return redirect(url_for("login", next=request.path))
            if current_user()["role"] not in roles:
                abort(403)
            return view(*args, **kwargs)

        return wrapped

    return decorator


def _active_franchise_required():
    user = current_user()
    if user and user["role"] != "super_admin":
        franchise = fetch_one("SELECT * FROM franchises WHERE id=%s", (user["franchise_id"],))
        if franchise and not boolish(franchise.get("active", 1)):
            session.clear()
            flash("This client account is inactive. Please contact the platform administrator.", "error")
            return redirect(url_for("login"))
    return None


@app.route("/health")
def health():
    return {"status": "ok", "database": "error" if DATABASE_INIT_ERROR else "ready"}


@app.route("/")
def home():
    return render_template("public_home.html", franchises=visible_franchises(), branches=visible_branches(public_only=True))


def _render_public_booking(preselected_branch=None):
    if request.method == "POST":
        branch = branch_by_id(request.form.get("branch_id")) if request.form.get("branch_id") else preselected_branch
        if not branch or not boolish(branch.get("public_booking_enabled", 1)):
            flash("Please choose a valid branch before submitting your booking.", "error")
        else:
            reference = insert_booking(branch, request.form, "Website", "Pending")
            flash(f"Booking {reference} has been created.", "success")
            return redirect(url_for("booking_success", reference=reference))

    return render_template(
        "public_booking.html",
        franchises=visible_franchises(),
        branches=visible_branches(public_only=True),
        preselected_branch=preselected_branch,
    )


@app.route("/book", methods=["GET", "POST"])
def public_booking():
    return _render_public_booking()


@app.route("/book/<franchise_slug>/<branch_slug>", methods=["GET", "POST"])
def public_branch_booking(franchise_slug, branch_slug):
    branch = branch_for_public_booking(franchise_slug, branch_slug)
    if not branch:
        abort(404)
    return _render_public_booking(branch)


@app.route("/booking-success/<reference>")
def booking_success(reference):
    booking = fetch_one(
        """
        SELECT b.*, br.name AS branch_name, f.name AS franchise_name
        FROM bookings b
        LEFT JOIN branches br ON br.id = b.branch_id
        LEFT JOIN franchises f ON f.id = b.franchise_id
        WHERE b.booking_reference=%s
        """,
        (reference,),
    )
    if not booking:
        abort(404)
    return render_template("booking_success.html", booking=booking)


@app.route("/login", methods=["GET", "POST"])
def login():
    if local_database_unavailable():
        return render_template("login.html", error="The local database is unavailable. Set SQLITE_PATH to a writable path or use DATABASE_URL.")
    if current_user():
        return redirect(url_for("dashboard"))

    error = None
    if request.method == "POST":
        username = (request.form.get("username") or "").strip()
        password = request.form.get("password") or ""
        user = fetch_one("SELECT * FROM users WHERE lower(username)=lower(%s)", (username,))
        valid = False
        if user:
            if user.get("password_hash"):
                valid = check_password_hash(user["password_hash"], password)
            elif user.get("password"):
                valid = password == user["password"]
                if valid:
                    execute_db("UPDATE users SET password_hash=%s, password=%s, updated_at=%s WHERE id=%s", (generate_password_hash(password), "", utc_now(), user["id"]))
        if valid and boolish(user.get("active", 1)):
            session.clear()
            session["user_id"] = user["id"]
            flash(f"Welcome back, {user.get('full_name') or user['username']}.", "success")
            if boolish(user.get("must_reset_password")):
                flash("This account is using a legacy or temporary password. Please change it now.", "info")
                return redirect(url_for("change_password"))
            return redirect(request.args.get("next") or url_for("dashboard"))
        error = "Invalid username or password."

    return render_template("login.html", error=error)


@app.route("/account/password", methods=["GET", "POST"])
@login_required
def change_password():
    error = None
    if request.method == "POST":
        current_password = request.form.get("current_password") or ""
        new_password = request.form.get("new_password") or ""
        confirm_password = request.form.get("confirm_password") or ""
        user = current_user()

        valid = False
        if user.get("password_hash"):
            valid = check_password_hash(user["password_hash"], current_password)
        elif user.get("password"):
            valid = current_password == user["password"]

        if not valid:
            error = "Current password is incorrect."
        elif len(new_password) < 10:
            error = "Use at least 10 characters for the new password."
        elif new_password != confirm_password:
            error = "The new passwords do not match."
        elif new_password == current_password:
            error = "Choose a different password from the current one."
        else:
            execute_db(
                "UPDATE users SET password=%s, password_hash=%s, must_reset_password=0, updated_at=%s WHERE id=%s",
                ("", generate_password_hash(new_password), utc_now(), user["id"]),
            )
            flash("Password updated successfully.", "success")
            return redirect(url_for("dashboard"))

    return render_template("password_reset.html", error=error)


@app.route("/signup")
def signup_redirect():
    flash("Franchise setup is now handled inside the platform by admin users.", "info")
    return redirect(url_for("login"))


@app.route("/logout")
def logout():
    session.clear()
    flash("You have been logged out.", "success")
    return redirect(url_for("home"))


@app.route("/dashboard")
@login_required
def dashboard():
    inactive_redirect = _active_franchise_required()
    if inactive_redirect:
        return inactive_redirect
    bookings = fetch_visible_bookings(current_user())
    reminders = fetch_reminders_for_user(current_user())
    today = utc_today()
    franchise = fetch_one("SELECT * FROM franchises WHERE id=%s", (current_user().get("franchise_id"),)) if current_user()["role"] != "super_admin" else None
    monthly_rows = monthly_usage_summary(current_user())
    latest_monthly = monthly_rows[0] if monthly_rows else None
    return render_template(
        "dashboard.html",
        today_bookings=[item for item in bookings if item.get("scheduled_date") == today],
        recent_bookings=bookings[:12],
        metrics={
            "total": len(bookings),
            "today": len([item for item in bookings if item.get("scheduled_date") == today]),
            "pending": len([item for item in bookings if item.get("status") in {"Pending", "Confirmed", "In Progress"}]),
            "completed": len([item for item in bookings if item.get("status") in DONE_STATUSES]),
            "reminders": len([item for item in reminders if item.get("status") == "Pending"]),
        },
        branch_summaries=visible_branches(user=current_user()),
        franchise=franchise,
        plan_features_list=plan_features(franchise) if franchise else [],
        latest_monthly=latest_monthly,
        monthly_usage=monthly_rows,
    )


@app.route("/bookings")
@login_required
def bookings():
    inactive_redirect = _active_franchise_required()
    if inactive_redirect:
        return inactive_redirect
    filters = {
        "search": request.args.get("search", ""),
        "status": request.args.get("status", ""),
        "scheduled_date": iso_date(request.args.get("scheduled_date", "")),
        "branch_id": request.args.get("branch_id", ""),
        "franchise_id": request.args.get("franchise_id", ""),
    }
    return render_template(
        "bookings.html",
        bookings=fetch_visible_bookings(current_user(), filters),
        filters=filters,
        branch_options=visible_branches(user=current_user()),
        franchise_options=visible_franchises(user=current_user()),
    )


@app.route("/bookings/<reference>")
@login_required
def booking_detail(reference):
    booking = fetch_booking_for_user(reference, current_user())
    if not booking:
        abort(404)
    history = fetch_all("SELECT * FROM communication_logs WHERE booking_id=%s ORDER BY created_at DESC", (booking["id"],))
    return render_template("booking_detail.html", booking=booking, communication_history=history, branch_options=visible_branches(user=current_user()))


@app.route("/bookings/<reference>/quick-update", methods=["POST"])
@login_required
def quick_update_booking(reference):
    booking = fetch_booking_for_user(reference, current_user())
    if not booking:
        abort(404)
    status = request.form.get("status") or booking.get("status")
    quote_declined = request.form.get("quote_declined") or booking.get("quote_declined") or "No"
    completed_at = booking.get("completed_at") if status in DONE_STATUSES else ""
    completed_at = completed_at or (utc_today() if status in DONE_STATUSES else "")
    service_due_date = __import__("platform_helpers").compute_service_due_date(booking.get("service_level"), completed_at)
    execute_db("UPDATE bookings SET status=%s, quote_declined=%s, completed_at=%s, service_due_date=%s, updated_at=%s WHERE id=%s", (status, quote_declined, completed_at, service_due_date, utc_now(), booking["id"]))
    flash(f"Booking {reference} updated.", "success")
    return redirect(request.referrer or url_for("bookings"))


@app.route("/bookings/<reference>/update", methods=["POST"])
@login_required
def update_booking(reference):
    booking = fetch_booking_for_user(reference, current_user())
    if not booking:
        abort(404)
    branch = selected_branch_for_user(current_user(), request.form.get("branch_id")) or branch_by_id(booking["branch_id"])
    scheduled_date = iso_date(request.form.get("scheduled_date")) or booking.get("scheduled_date") or utc_today()
    service = (request.form.get("service") or booking.get("service") or "").strip()
    service_level = __import__("database").classify_service_level(service)
    status = request.form.get("status") or booking.get("status")
    completed_at = booking.get("completed_at") if status in DONE_STATUSES else ""
    completed_at = completed_at or (utc_today() if status in DONE_STATUSES else "")
    service_due_date = __import__("platform_helpers").compute_service_due_date(service_level, completed_at)
    execute_db(
        """
        UPDATE bookings
        SET franchise_id=%s, branch_id=%s, company=%s, branch=%s, first_name=%s, surname=%s, customer_email=%s,
            phone=%s, preferred_contact_method=%s, make=%s, model=%s, vehicle_year=%s, fuel_type=%s, vehicle_vin=%s,
            service=%s, service_level=%s, current_mileage=%s, scheduled_date=%s, date=%s, status=%s, service_due_date=%s,
            work_to_be_done=%s, public_notes=%s, internal_notes=%s, quote_declined=%s, reminder_opt_in=%s, completed_at=%s, updated_at=%s
        WHERE id=%s
        """,
        (
            branch["franchise_id"], branch["id"], branch["franchise_name"], branch["name"],
            (request.form.get("first_name") or "").strip(), (request.form.get("surname") or "").strip(),
            (request.form.get("customer_email") or "").strip(), (request.form.get("phone") or "").strip(),
            (request.form.get("preferred_contact_method") or "WhatsApp").strip(), (request.form.get("make") or "").strip(),
            (request.form.get("model") or "").strip(), (request.form.get("vehicle_year") or "").strip(),
            (request.form.get("fuel_type") or "").strip(), (request.form.get("vehicle_vin") or "").strip(), service,
            service_level, (request.form.get("current_mileage") or "").strip(), scheduled_date, scheduled_date, status,
            service_due_date, (request.form.get("work_to_be_done") or "").strip(), (request.form.get("public_notes") or "").strip(),
            (request.form.get("internal_notes") or "").strip(), (request.form.get("quote_declined") or "No").strip(),
            1 if boolish(request.form.get("reminder_opt_in", "true")) else 0, completed_at or None, utc_now(), booking["id"],
        ),
    )
    flash(f"Booking {reference} saved.", "success")
    return redirect(url_for("booking_detail", reference=reference))


@app.route("/add", methods=["GET", "POST"])
@login_required
def add_booking():
    inactive_redirect = _active_franchise_required()
    if inactive_redirect:
        return inactive_redirect
    if request.method == "POST":
        branch = selected_branch_for_user(current_user(), request.form.get("branch_id"))
        if branch:
            reference = insert_booking(branch, request.form, "Reception", "Confirmed")
            flash(f"Reception booking {reference} created.", "success")
            return redirect(url_for("booking_detail", reference=reference))
        flash("Please choose a valid branch.", "error")
    return render_template("booking_form.html", page_title="Reception Booking", submit_label="Save Booking", source_label="Reception booking", default_values={"scheduled_date": utc_today(), "preferred_contact_method": "WhatsApp"}, branch_options=visible_branches(user=current_user()), lock_branch=current_user()["role"] == "reception", prices=fetch_service_prices(current_user()))


@app.route("/walkin", methods=["GET", "POST"])
@login_required
def walkin():
    inactive_redirect = _active_franchise_required()
    if inactive_redirect:
        return inactive_redirect
    if request.method == "POST":
        branch = selected_branch_for_user(current_user(), request.form.get("branch_id"))
        if branch:
            reference = insert_booking(branch, request.form, "Walk-in", "In Progress")
            flash(f"Walk-in {reference} recorded.", "success")
            return redirect(url_for("booking_detail", reference=reference))
        flash("Please choose a valid branch.", "error")
    return render_template("booking_form.html", page_title="Workshop Walk-In", submit_label="Save Walk-In", source_label="Walk-in", default_values={"scheduled_date": utc_today(), "preferred_contact_method": "WhatsApp"}, branch_options=visible_branches(user=current_user()), lock_branch=current_user()["role"] == "reception", prices=fetch_service_prices(current_user()))


@app.route("/customers")
@login_required
def customers():
    inactive_redirect = _active_franchise_required()
    if inactive_redirect:
        return inactive_redirect
    customer_map = {}
    for booking in fetch_visible_bookings(current_user()):
        key = (booking.get("phone") or booking.get("customer_email") or booking["booking_reference"]).strip()
        customer_map.setdefault(key, {"name": f"{booking.get('first_name', '')} {booking.get('surname', '')}".strip() or "Unknown", "phone": booking.get("phone") or "", "email": booking.get("customer_email") or "", "branch_name": booking.get("branch_name") or "", "latest_booking": booking.get("booking_reference")})
    return render_template("customers.html", customers=sorted(customer_map.values(), key=lambda item: item["name"].lower()))


@app.route("/customers/<phone>")
@login_required
def customer_history(phone):
    return render_template("customer_history.html", phone=phone, bookings=[item for item in fetch_visible_bookings(current_user()) if (item.get("phone") or "") == phone])


@app.route("/reports")
@login_required
def reports():
    inactive_redirect = _active_franchise_required()
    if inactive_redirect:
        return inactive_redirect
    bookings = fetch_visible_bookings(current_user())
    by_status = {status: len([item for item in bookings if item.get("status") == status]) for status in STATUS_OPTIONS}
    by_service = {"Major": 0, "Minor": 0, "General": 0}
    for item in bookings:
        by_service[item.get("service_level") or "General"] = by_service.get(item.get("service_level") or "General", 0) + 1
    return render_template("reports.html", total=len(bookings), by_status=by_status, by_service=by_service, reminders=fetch_reminders_for_user(current_user()))


@app.route("/reminders")
@login_required
def reminders():
    inactive_redirect = _active_franchise_required()
    if inactive_redirect:
        return inactive_redirect
    created = generate_due_reminders(current_user())
    if created:
        flash(f"{created} reminder campaign(s) were generated for the current month-end window.", "success")
    return render_template("reminders.html", reminders=fetch_reminders_for_user(current_user()))


@app.route("/reminders/run", methods=["POST"])
@login_required
def run_reminders():
    created = generate_due_reminders(current_user(), force=boolish(request.form.get("force")))
    sent = 0
    for reminder in fetch_reminders_for_user(current_user()) if boolish(request.form.get("send_now")) else []:
        if reminder.get("status") == "Pending":
            success, _message = auto_send_reminder(reminder, current_user())
            if success:
                sent += 1
    flash(f"Generated {created} reminder campaign(s).", "success")
    if sent:
        flash(f"Automatically sent {sent} reminder(s).", "success")
    elif boolish(request.form.get("send_now")):
        flash("No direct channel provider was configured, so the reminders are ready for manual sending.", "info")
    return redirect(url_for("reminders"))


@app.route("/reminders/<int:reminder_id>/send/<channel>")
@login_required
def send_reminder(reminder_id, channel):
    reminder = fetch_reminder(reminder_id)
    if channel not in {"email", "sms", "whatsapp"} or not reminder or not reminder_in_scope(reminder, current_user()):
        abort(404)
    booking = fetch_one("SELECT b.*, f.name AS franchise_name, f.slug AS franchise_slug, br.name AS branch_name, br.slug AS branch_slug, br.contact_email AS branch_contact_email, br.contact_phone AS branch_contact_phone FROM bookings b LEFT JOIN franchises f ON f.id = b.franchise_id LEFT JOIN branches br ON br.id = b.branch_id WHERE b.id=%s", (reminder["booking_id"],))
    subject, body = build_booking_message(booking, reminder)
    recipient = booking.get("customer_email") if channel == "email" else booking.get("phone")
    if not recipient:
        flash("This customer does not have the required contact details for that channel.", "error")
        return redirect(url_for("reminders"))
    try:
        if channel == "email" and smtp_configured():
            send_email_message(recipient, subject, body)
            log_communication(booking, reminder, channel, recipient, subject, body, "sent", current_user()["id"])
            update_reminder_status(reminder_id, "Sent", channel, count_as_send=True)
            flash("Email sent successfully.", "success")
            return redirect(url_for("reminders"))
        if channel in {"sms", "whatsapp"} and twilio_configured(channel):
            send_twilio_message(channel, recipient, body)
            log_communication(booking, reminder, channel, recipient, subject, body, "sent", current_user()["id"])
            update_reminder_status(reminder_id, "Sent", channel, count_as_send=True)
            flash(f"{channel.title()} message sent successfully.", "success")
            return redirect(url_for("reminders"))
    except Exception as exc:
        log_communication(booking, reminder, channel, recipient, subject, body, f"failed: {exc}", current_user()["id"])
        flash(f"Direct sending failed: {exc}", "error")
        return redirect(url_for("reminders"))
    link = manual_channel_link(channel, recipient, subject, body)
    log_communication(booking, reminder, channel, recipient, subject, body, "manual_open", current_user()["id"], link)
    update_reminder_status(reminder_id, "Prepared", channel)
    return redirect(link)


@app.route("/manage/franchises", methods=["GET", "POST"])
@roles_required("super_admin")
def manage_franchises():
    if request.method == "POST":
        name = (request.form.get("name") or "").strip()
        if name and not fetch_one("SELECT id FROM franchises WHERE lower(name)=lower(%s)", (name,)):
            plan_code = (request.form.get("plan_code") or "basic").lower()
            plan = PLAN_DEFINITIONS.get(plan_code, PLAN_DEFINITIONS["basic"])
            execute_db(
                """
                INSERT INTO franchises (
                    name, slug, contact_email, contact_phone, notes, plan_code, branch_limit, user_limit,
                    automation_enabled, chatbot_enabled, reporting_enabled, custom_integrations_enabled,
                    priority_support_enabled, monthly_base_price, monthly_message_limit, overage_price_per_message,
                    billing_day, active, created_at, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'month_end', 1, %s, %s)
                """,
                (
                    name,
                    __import__('database').slugify(name),
                    (request.form.get("contact_email") or "").strip(),
                    (request.form.get("contact_phone") or "").strip(),
                    (request.form.get("notes") or "").strip(),
                    plan_code,
                    plan["branch_limit"],
                    plan["user_limit"],
                    plan["automation_enabled"],
                    plan["chatbot_enabled"],
                    plan["reporting_enabled"],
                    plan["custom_integrations_enabled"],
                    plan["priority_support_enabled"],
                    float(request.form.get("monthly_base_price") or 0),
                    int(request.form.get("monthly_message_limit") or 2000),
                    float(request.form.get("overage_price_per_message") or 0.5),
                    utc_now(),
                    utc_now(),
                ),
            )
            flash(f"Franchise {name} created.", "success")
        else:
            flash("Please use a unique franchise name.", "error")
    franchises = visible_franchises(include_inactive=True)
    counts = {item["id"]: franchise_counts(item["id"]) for item in franchises}
    return render_template("manage_franchises.html", franchises=franchises, franchise_counts=counts, monthly_usage=monthly_usage_summary(), daily_usage=daily_usage_summary())


@app.route("/manage/franchises/<int:franchise_id>/update", methods=["POST"])
@roles_required("super_admin")
def update_franchise(franchise_id):
    franchise = fetch_one("SELECT * FROM franchises WHERE id=%s", (franchise_id,))
    if not franchise:
        abort(404)
    plan_code = (request.form.get("plan_code") or franchise.get("plan_code") or "basic").lower()
    plan = PLAN_DEFINITIONS.get(plan_code, PLAN_DEFINITIONS["basic"])
    execute_db(
        """
        UPDATE franchises
        SET contact_email=%s, contact_phone=%s, notes=%s, plan_code=%s, branch_limit=%s, user_limit=%s,
            automation_enabled=%s, chatbot_enabled=%s, reporting_enabled=%s, custom_integrations_enabled=%s,
            priority_support_enabled=%s, monthly_base_price=%s, monthly_message_limit=%s, overage_price_per_message=%s,
            active=%s, updated_at=%s
        WHERE id=%s
        """,
        (
            (request.form.get("contact_email") or "").strip(),
            (request.form.get("contact_phone") or "").strip(),
            (request.form.get("notes") or "").strip(),
            plan_code,
            plan["branch_limit"] if plan_code != "premium" else 999999,
            plan["user_limit"] if plan_code != "premium" else 999999,
            plan["automation_enabled"],
            plan["chatbot_enabled"],
            plan["reporting_enabled"],
            plan["custom_integrations_enabled"],
            plan["priority_support_enabled"],
            float(request.form.get("monthly_base_price") or 0),
            int(request.form.get("monthly_message_limit") or 2000),
            float(request.form.get("overage_price_per_message") or 0.5),
            1 if boolish(request.form.get("active", "true")) else 0,
            utc_now(),
            franchise_id,
        ),
    )
    flash(f"Updated {franchise['name']}.", "success")
    return redirect(url_for("manage_franchises"))


@app.route("/manage/branches", methods=["GET", "POST"])
@roles_required("franchise_admin", "super_admin")
def manage_branches():
    if request.method == "POST":
        franchise_id = request.form.get("franchise_id") or current_user().get("franchise_id")
        if current_user()["role"] != "super_admin":
            franchise_id = current_user()["franchise_id"]
        franchise = fetch_one("SELECT * FROM franchises WHERE id=%s", (franchise_id,))
        name = (request.form.get("name") or "").strip()
        if franchise and not can_add_branch(franchise):
            flash(f"{franchise['name']} has reached its branch limit for the {plan_label(franchise.get('plan_code'))} plan.", "error")
        elif franchise and name and not fetch_one("SELECT id FROM branches WHERE franchise_id=%s AND lower(name)=lower(%s)", (franchise["id"], name)):
            execute_db("INSERT INTO branches (franchise_id, name, slug, code, location, contact_email, contact_phone, public_booking_enabled, active, created_at, updated_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 1, %s, %s)", (franchise["id"], name, __import__('database').slugify(name), (request.form.get("code") or "").strip(), (request.form.get("location") or "").strip(), (request.form.get("contact_email") or "").strip(), (request.form.get("contact_phone") or "").strip(), 1 if boolish(request.form.get("public_booking_enabled", "true")) else 0, utc_now(), utc_now()))
            flash(f"Branch {name} created.", "success")
        else:
            flash("Please provide a unique branch name for that franchise.", "error")
    branch_counts = {
        row["branch_id"]: row["total"]
        for row in fetch_all("SELECT branch_id, COUNT(*) AS total FROM bookings GROUP BY branch_id")
    }
    return render_template(
        "manage_branches.html",
        branches=visible_branches(user=current_user(), include_inactive=True),
        franchises=visible_franchises(user=current_user(), include_inactive=True),
        branch_counts=branch_counts,
    )


@app.route("/manage/branches/<int:branch_id>/move", methods=["POST"])
@roles_required("super_admin")
def move_branch(branch_id):
    branch = branch_by_id(branch_id)
    target_franchise = fetch_one("SELECT * FROM franchises WHERE id=%s", (request.form.get("franchise_id"),))
    if not branch or not target_franchise:
        abort(404)

    execute_db(
        "UPDATE branches SET franchise_id=%s, updated_at=%s WHERE id=%s",
        (target_franchise["id"], utc_now(), branch_id),
    )
    execute_db(
        "UPDATE bookings SET franchise_id=%s, company=%s, updated_at=%s WHERE branch_id=%s",
        (target_franchise["id"], target_franchise["name"], utc_now(), branch_id),
    )
    execute_db(
        "UPDATE users SET franchise_id=%s, company=%s, updated_at=%s WHERE branch_id=%s",
        (target_franchise["id"], target_franchise["name"], utc_now(), branch_id),
    )
    flash(f"Moved {branch['name']} into {target_franchise['name']} and updated linked users and bookings.", "success")
    return redirect(url_for("manage_branches"))


@app.route("/manage/users", methods=["GET", "POST"])
@roles_required("franchise_admin", "super_admin")
def manage_users():
    if request.method == "POST":
        username = (request.form.get("username") or "").strip()
        password = request.form.get("password") or ""
        role = request.form.get("role") or "reception"
        if role in available_roles_for_creator(current_user()) and username and password and not fetch_one("SELECT id FROM users WHERE lower(username)=lower(%s)", (username,)):
            franchise_id = request.form.get("franchise_id") or current_user().get("franchise_id")
            branch_id = request.form.get("branch_id") or None
            branch = selected_branch_for_user(current_user(), branch_id) if role == "reception" else None
            franchise = fetch_one("SELECT * FROM franchises WHERE id=%s", (branch["franchise_id"] if branch else franchise_id,))
            if franchise and not can_add_user(franchise):
                flash(f"{franchise['name']} has reached its user limit for the {plan_label(franchise.get('plan_code'))} plan.", "error")
                scope_sql, args = user_scope_clause(current_user())
                users = fetch_all("SELECT u.*, f.name AS franchise_name, b.name AS branch_name FROM users u LEFT JOIN franchises f ON f.id = u.franchise_id LEFT JOIN branches b ON b.id = u.branch_id WHERE " + scope_sql + " ORDER BY u.role, u.username", tuple(args))
                return render_template("manage_users.html", users=users, roles=available_roles_for_creator(current_user()), branches=visible_branches(user=current_user(), include_inactive=True), franchises=visible_franchises(user=current_user(), include_inactive=True))
            if role == "reception" and not branch:
                flash("Reception users must be linked to a visible branch.", "error")
            else:
                company_name = branch["franchise_name"] if branch else (franchise or {}).get("name", "")
                execute_db("INSERT INTO users (username, password, password_hash, full_name, email, phone, branch, company, role, franchise_id, branch_id, active, must_reset_password, created_at, updated_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 1, 0, %s, %s)", (username, "", generate_password_hash(password), (request.form.get("full_name") or username.title()).strip(), (request.form.get("email") or "").strip(), (request.form.get("phone") or "").strip(), branch["name"] if branch else "", company_name, role, branch["franchise_id"] if branch else (None if role == "super_admin" else franchise_id), branch["id"] if branch else None, utc_now(), utc_now()))
                flash(f"User {username} created.", "success")
        else:
            flash("Please provide a unique username, a password, and a valid role.", "error")
    scope_sql, args = user_scope_clause(current_user())
    users = fetch_all("SELECT u.*, f.name AS franchise_name, b.name AS branch_name FROM users u LEFT JOIN franchises f ON f.id = u.franchise_id LEFT JOIN branches b ON b.id = u.branch_id WHERE " + scope_sql + " ORDER BY u.role, u.username", tuple(args))
    return render_template("manage_users.html", users=users, roles=available_roles_for_creator(current_user()), branches=visible_branches(user=current_user(), include_inactive=True), franchises=visible_franchises(user=current_user(), include_inactive=True))


@app.route("/manage/users/<int:user_id>/assign", methods=["POST"])
@roles_required("franchise_admin", "super_admin")
def assign_user(user_id):
    candidate = fetch_one("SELECT * FROM users WHERE id=%s", (user_id,))
    if not candidate:
        abort(404)
    if current_user()["role"] != "super_admin" and candidate.get("franchise_id") != current_user().get("franchise_id"):
        abort(403)

    role = request.form.get("role") or candidate["role"]
    if role not in available_roles_for_creator(current_user()) and role != candidate["role"]:
        flash("That role is not available for your account.", "error")
        return redirect(url_for("manage_users"))

    branch = None
    franchise = None
    branch_id = request.form.get("branch_id") or None
    franchise_id = request.form.get("franchise_id") or candidate.get("franchise_id")

    if role == "reception":
        branch = selected_branch_for_user(current_user(), branch_id)
        if not branch:
            flash("Reception users must be assigned to a visible branch.", "error")
            return redirect(url_for("manage_users"))
        franchise = fetch_one("SELECT * FROM franchises WHERE id=%s", (branch["franchise_id"],))
    elif role != "super_admin":
        franchise = fetch_one("SELECT * FROM franchises WHERE id=%s", (franchise_id,))
        if not franchise:
            flash("Please choose a valid franchise.", "error")
            return redirect(url_for("manage_users"))

    execute_db(
        """
        UPDATE users
        SET role=%s,
            franchise_id=%s,
            branch_id=%s,
            branch=%s,
            company=%s,
            updated_at=%s
        WHERE id=%s
        """,
        (
            role,
            branch["franchise_id"] if branch else (franchise["id"] if franchise else None),
            branch["id"] if branch else None,
            branch["name"] if branch else "",
            branch["franchise_name"] if branch else (franchise["name"] if franchise else ""),
            utc_now(),
            user_id,
        ),
    )
    flash(f"Updated assignment for {candidate['username']}.", "success")
    return redirect(url_for("manage_users"))


@app.route("/manage/users/<int:user_id>/toggle", methods=["POST"])
@roles_required("franchise_admin", "super_admin")
def toggle_user(user_id):
    candidate = fetch_one("SELECT * FROM users WHERE id=%s", (user_id,))
    if not candidate:
        abort(404)
    if current_user()["role"] != "super_admin" and candidate.get("franchise_id") != current_user().get("franchise_id"):
        abort(403)
    execute_db("UPDATE users SET active=%s, updated_at=%s WHERE id=%s", (0 if boolish(candidate.get("active", 1)) else 1, utc_now(), user_id))
    flash(f"Updated {candidate['username']}.", "success")
    return redirect(url_for("manage_users"))


@app.route("/manage/users/<int:user_id>/password", methods=["POST"])
@roles_required("franchise_admin", "super_admin")
def reset_user_password(user_id):
    candidate = fetch_one("SELECT * FROM users WHERE id=%s", (user_id,))
    if not candidate:
        abort(404)
    if current_user()["role"] != "super_admin" and candidate.get("franchise_id") != current_user().get("franchise_id"):
        abort(403)
    password = request.form.get("password") or ""
    if not password:
        flash("Password cannot be empty.", "error")
    else:
        execute_db("UPDATE users SET password_hash=%s, password=%s, must_reset_password=%s, updated_at=%s WHERE id=%s", (generate_password_hash(password), "", 1 if boolish(request.form.get("must_reset_password")) else 0, utc_now(), user_id))
        flash(f"Password reset for {candidate['username']}.", "success")
    return redirect(url_for("manage_users"))


@app.route("/manage/prices", methods=["GET", "POST"])
@roles_required("franchise_admin", "super_admin")
def manage_prices():
    if request.method == "POST":
        franchise_id = request.form.get("franchise_id") or current_user().get("franchise_id")
        if current_user()["role"] != "super_admin":
            franchise_id = current_user()["franchise_id"]
        execute_db(
            "INSERT INTO service_prices (franchise_id, branch_id, service_name, service_category, price_amount, active, created_at, updated_at) VALUES (%s, %s, %s, %s, %s, 1, %s, %s)",
            (
                franchise_id,
                request.form.get("branch_id") or None,
                (request.form.get("service_name") or "").strip(),
                (request.form.get("service_category") or "").strip(),
                float(request.form.get("price_amount") or 0),
                utc_now(),
                utc_now(),
            ),
        )
        flash("Service price saved.", "success")
    return render_template("manage_prices.html", prices=fetch_service_prices(current_user()), branches=visible_branches(user=current_user(), include_inactive=True), franchises=visible_franchises(user=current_user(), include_inactive=True))


@app.route("/chatbot/inbox", methods=["GET", "POST"])
@roles_required("franchise_admin", "super_admin")
def chatbot_inbox():
    if request.method == "POST":
        franchise_id = request.form.get("franchise_id") or current_user().get("franchise_id")
        if current_user()["role"] != "super_admin":
            franchise_id = current_user()["franchise_id"]
        branch = branch_by_id(request.form.get("branch_id")) if request.form.get("branch_id") else None
        franchise = fetch_one("SELECT * FROM franchises WHERE id=%s", (franchise_id,))
        service_name = (request.form.get("suggested_service") or "").strip()
        matched_price = None
        if branch and franchise:
            price_match = find_service_price(franchise["id"], branch["id"], service_name)
            matched_price = (price_match or {}).get("price_amount")
        execute_db(
            "INSERT INTO chatbot_messages (franchise_id, branch_id, customer_name, customer_phone, customer_email, channel, direction, message_text, suggested_service, matched_price, status, processed, created_at, updated_at) VALUES (%s, %s, %s, %s, %s, %s, 'inbound', %s, %s, %s, 'Saved', 0, %s, %s)",
            (
                franchise_id,
                branch["id"] if branch else None,
                (request.form.get("customer_name") or "").strip(),
                (request.form.get("customer_phone") or "").strip(),
                (request.form.get("customer_email") or "").strip(),
                (request.form.get("channel") or "WhatsApp").strip(),
                (request.form.get("message_text") or "").strip(),
                service_name,
                matched_price,
                utc_now(),
                utc_now(),
            ),
        )
        _record_chatbot_usage(franchise_id)
        flash("Message saved to inbox for processing.", "success")
    messages = fetch_all(
        """
        SELECT cm.*, f.name AS franchise_name, b.name AS branch_name
        FROM chatbot_messages cm
        LEFT JOIN franchises f ON f.id = cm.franchise_id
        LEFT JOIN branches b ON b.id = cm.branch_id
        """
        + (" WHERE cm.franchise_id=%s" if current_user()["role"] != "super_admin" else "")
        + " ORDER BY cm.created_at DESC",
        (current_user()["franchise_id"],) if current_user()["role"] != "super_admin" else (),
    )
    return render_template("chatbot_inbox.html", messages=messages, branches=visible_branches(user=current_user(), include_inactive=True), franchises=visible_franchises(user=current_user(), include_inactive=True), daily_usage=daily_usage_summary(current_user()), monthly_usage=monthly_usage_summary(current_user()))


def _record_chatbot_usage(franchise_id):
    today = utc_today()
    month_key = today[:7]
    franchise = fetch_one("SELECT * FROM franchises WHERE id=%s", (franchise_id,))
    if not franchise:
        return
    daily = fetch_one("SELECT * FROM chatbot_usage_daily WHERE franchise_id=%s AND usage_date=%s", (franchise_id, today))
    if daily:
        execute_db("UPDATE chatbot_usage_daily SET message_count=%s, updated_at=%s WHERE id=%s", (int(daily.get("message_count") or 0) + 1, utc_now(), daily["id"]))
    else:
        execute_db("INSERT INTO chatbot_usage_daily (franchise_id, usage_date, message_count, created_at, updated_at) VALUES (%s, %s, 1, %s, %s)", (franchise_id, today, utc_now(), utc_now()))
    monthly = fetch_one("SELECT * FROM chatbot_usage_monthly WHERE franchise_id=%s AND usage_month=%s", (franchise_id, month_key))
    if monthly:
        message_count = int(monthly.get("message_count") or 0) + 1
        limit = int(monthly.get("message_limit") or franchise.get("monthly_message_limit") or 2000)
        overage_price = float(monthly.get("overage_price") or franchise.get("overage_price_per_message") or 0.5)
        extra = max(message_count - limit, 0)
        overage_cost = extra * overage_price
        total_due = float(monthly.get("base_price") or franchise.get("monthly_base_price") or 0) + overage_cost
        execute_db("UPDATE chatbot_usage_monthly SET message_count=%s, extra_messages=%s, overage_cost=%s, total_due=%s, updated_at=%s WHERE id=%s", (message_count, extra, overage_cost, total_due, utc_now(), monthly["id"]))
    else:
        limit = int(franchise.get("monthly_message_limit") or 2000)
        base_price = float(franchise.get("monthly_base_price") or 0)
        overage_price = float(franchise.get("overage_price_per_message") or 0.5)
        execute_db(
            "INSERT INTO chatbot_usage_monthly (franchise_id, usage_month, message_count, message_limit, extra_messages, base_price, overage_price, overage_cost, total_due, created_at, updated_at) VALUES (%s, %s, 1, %s, 0, %s, %s, 0, %s, %s, %s)",
            (franchise_id, month_key, limit, base_price, overage_price, base_price, utc_now(), utc_now()),
        )


@app.route("/billing/close-month", methods=["POST"])
@roles_required("super_admin")
def close_billing_month():
    usage_month = (request.form.get("usage_month") or utc_today()[:7]).strip()
    rows = fetch_all("SELECT cum.*, f.monthly_base_price, f.monthly_message_limit, f.overage_price_per_message FROM chatbot_usage_monthly cum LEFT JOIN franchises f ON f.id = cum.franchise_id WHERE cum.usage_month=%s", (usage_month,))
    for row in rows:
        limit = int(row.get("message_limit") or row.get("monthly_message_limit") or 2000)
        overage_price = float(row.get("overage_price") or row.get("overage_price_per_message") or 0.5)
        base_price = float(row.get("base_price") or row.get("monthly_base_price") or 0)
        extra = max(int(row.get("message_count") or 0) - limit, 0)
        overage_cost = extra * overage_price
        total_due = base_price + overage_cost
        execute_db("UPDATE chatbot_usage_monthly SET message_limit=%s, extra_messages=%s, base_price=%s, overage_price=%s, overage_cost=%s, total_due=%s, updated_at=%s WHERE id=%s", (limit, extra, base_price, overage_price, overage_cost, total_due, utc_now(), row["id"]))
    flash(f"Closed billing calculations for {usage_month}.", "success")
    return redirect(url_for("manage_franchises"))


@app.route("/billing/<int:billing_id>/payment", methods=["POST"])
@roles_required("super_admin")
def update_billing_payment(billing_id):
    billing = fetch_one("SELECT * FROM chatbot_usage_monthly WHERE id=%s", (billing_id,))
    if not billing:
        abort(404)
    status = request.form.get("payment_status") or "Unpaid"
    paid_at = utc_now() if status == "Paid" else None
    execute_db(
        "UPDATE chatbot_usage_monthly SET payment_status=%s, paid_at=%s, payment_reference=%s, updated_at=%s WHERE id=%s",
        (status, paid_at, (request.form.get("payment_reference") or "").strip(), utc_now(), billing_id),
    )
    flash("Billing payment status updated.", "success")
    return redirect(url_for("manage_franchises"))


@app.errorhandler(403)
def forbidden(_error):
    return render_template("error.html", title="Access Denied", message="You do not have permission to view that page."), 403


@app.errorhandler(404)
def not_found(_error):
    return render_template("error.html", title="Page Not Found", message="We could not find the page you requested."), 404

from assistant_engine import assistant_reply
from platform_helpers import branch_by_id
from platform_messaging import send_twilio_message

@app.route("/webhook/twilio", methods=["POST"])
def twilio_webhook():
    phone = request.form.get("From")
    message = request.form.get("Body")

    branch = branch_by_id(1)  # upgrade later

    reply, should_count = assistant_reply(phone, message, branch)

    if reply:
        send_twilio_message("whatsapp", phone, reply)

    if should_count:
        _record_chatbot_usage(branch["franchise_id"])

    return "OK"
def is_date_available(branch_id, date):
    capacity = fetch_one("SELECT daily_capacity FROM branches WHERE id=%s", (branch_id,))["daily_capacity"]

    count = fetch_one("""
        SELECT COUNT(*) as total 
        FROM bookings 
        WHERE branch_id=%s AND scheduled_date=%s
    """, (branch_id, date))["total"]

    return count < capacity


if __name__ == "__main__":
    app.run(debug=True)
