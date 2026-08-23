from flask import Blueprint, render_template, request, redirect, url_for, flash
from database import query_db, execute_db, utc_now
from services.auth_service import login_required, active_location_required, current_user

settings_bp = Blueprint("settings", __name__)



@settings_bp.route("/settings")
@settings_bp.route("/settings/")
@login_required
def settings_overview():
    return redirect(url_for('settings.settings_business'))

# Business information settings
@settings_bp.route("/settings/business", methods=["GET", "POST"])
@login_required
def settings_business():
    inactive_redirect = active_location_required()
    if inactive_redirect:
        return inactive_redirect
    user = current_user()
    location_id = user["location_id"]

    if request.method == "GET":
        # Get business information
        business = query_db("SELECT * FROM locations WHERE id = %s", (location_id,), one=True)
        if not business:
            # Create a default location record if it doesn't exist
            execute_db("""
                INSERT INTO locations (id, name, created_at, updated_at)
                VALUES (%s, %s, %s, %s)
            """, (location_id, f"Workshop {location_id}", utc_now(), utc_now()))
            business = query_db("SELECT * FROM locations WHERE id = %s", (location_id,), one=True)

        # Get timezones for dropdown
        timezones = [
            {"value": "UTC", "label": "UTC"},
            {"value": "America/New_York", "label": "Eastern Time (New York)"},
            {"value": "America/Chicago", "label": "Central Time (Chicago)"},
            {"value": "America/Denver", "label": "Mountain Time (Denver)"},
            {"value": "America/Los_Angeles", "label": "Pacific Time (Los Angeles)"},
            {"value": "Europe/London", "label": "Greenwich Mean Time (London)"},
            {"value": "Europe/Paris", "label": "Central European Time (Paris)"},
            {"value": "Asia/Tokyo", "label": "Japan Standard Time (Tokyo)"},
            {"value": "Asia/Shanghai", "label": "China Standard Time (Shanghai)"},
            {"value": "Australia/Sydney", "label": "Australian Eastern Time (Sydney)"}
        ]

        return render_template("settings_business.html", business=business, timezones=timezones)

    elif request.method == "POST":
        # Process form submission
        form_data = request.form

        # Update business information
        updates = {}
        fields = [
            'name', 'trading_name', 'business_registration_number', 'vat_number',
            'contact_email', 'contact_phone', 'website', 'physical_address',
            'postal_address', 'province', 'city', 'workshop_type', 'timezone',
            'currency', 'language', 'description', 'public_base_url'
        ]

        aliases = {"email": "contact_email", "primary_whatsapp_number": "contact_phone", "secondary_phone_number": "contact_phone"}
        for field in fields + ["email", "primary_whatsapp_number", "secondary_phone_number"]:
            if field in form_data:
                value = form_data[field].strip()
                field = aliases.get(field, field)
                if value:  # Only update if not empty
                    updates[field] = value
                elif field in ['trading_name', 'business_registration_number', 'vat_number',
                              'secondary_phone_number', 'website', 'description']:
                    # These can be explicitly set to empty
                    updates[field] = ""

        if updates:
            updates["updated_at"] = utc_now()
            set_clause = ", ".join([f"{key}=%s" for key in updates.keys()])
            query = f"UPDATE locations SET {set_clause} WHERE id=%s"
            params = list(updates.values()) + [location_id]
            execute_db(query, tuple(params))

        flash('Business information updated successfully', 'success')
        return redirect(url_for('settings.settings_business'))

# Operating hours settings
@settings_bp.route("/settings/hours", methods=["GET", "POST"])
@login_required
def settings_hours():
    inactive_redirect = active_location_required()
    if inactive_redirect:
        return inactive_redirect
    location_id = current_user()["location_id"]
    location = query_db(
        "SELECT operating_hours_json FROM locations WHERE id=%s", (location_id,), one=True
    ) or {}
    import json
    defaults = {}
    for day in ("monday","tuesday","wednesday","thursday","friday"):
        defaults[f"{day}_enabled"] = True
        defaults[f"{day}_open"] = "08:00"
        defaults[f"{day}_close"] = "17:00"
    for day in ("saturday","sunday"):
        defaults[f"{day}_enabled"] = False
        defaults[f"{day}_open"] = "08:00"
        defaults[f"{day}_close"] = "13:00"
    defaults.update({"public_holidays":"closed","holiday_message":"","same_day_cutoff":"","advance_booking_days":90})
    try:
        saved = json.loads(location.get("operating_hours_json") or "{}")
        if isinstance(saved, dict):
            defaults.update(saved)
    except (TypeError, ValueError):
        pass

    if request.method == "POST":
        settings = dict(defaults)
        for day in ("monday","tuesday","wednesday","thursday","friday","saturday","sunday"):
            settings[f"{day}_enabled"] = request.form.get(f"{day}_enabled") == "on"
            settings[f"{day}_open"] = request.form.get(f"{day}_open","").strip()
            settings[f"{day}_close"] = request.form.get(f"{day}_close","").strip()
            if settings[f"{day}_open"] and not _is_valid_time(settings[f"{day}_open"]):
                flash(f"Invalid opening time for {day.title()}", "error")
                return redirect(url_for("settings.settings_hours"))
            if settings[f"{day}_close"] and not _is_valid_time(settings[f"{day}_close"]):
                flash(f"Invalid closing time for {day.title()}", "error")
                return redirect(url_for("settings.settings_hours"))
        settings["public_holidays"] = request.form.get("public_holidays","closed")
        settings["holiday_message"] = request.form.get("holiday_message","").strip()
        settings["same_day_cutoff"] = request.form.get("same_day_cutoff","").strip()
        try:
            settings["advance_booking_days"] = max(1, int(request.form.get("advance_booking_days","90")))
        except ValueError:
            settings["advance_booking_days"] = 90
        execute_db(
            "UPDATE locations SET operating_hours_json=%s, updated_at=%s WHERE id=%s",
            (json.dumps(settings), utc_now(), location_id),
        )
        flash("Operating hours updated successfully", "success")
        return redirect(url_for("settings.settings_hours"))
    return render_template("settings_hours.html", settings=defaults)


def _is_valid_time(time_str):
    try:
        hours, minutes = time_str.split(":", 1)
        return 0 <= int(hours) <= 23 and 0 <= int(minutes) <= 59
    except (ValueError, AttributeError):
        return False


@settings_bp.route("/settings/users", methods=["GET", "POST"])
@login_required
def settings_users():
    inactive_redirect = active_location_required()
    if inactive_redirect:
        return inactive_redirect
    user = current_user()
    location_id = user["location_id"]
    if user.get("role") not in {"owner", "admin"}:
        flash("Access denied. Administrator privileges required.", "error")
        return redirect(url_for("settings.settings_overview"))

    if request.method == "GET":
        users = query_db(
            """SELECT u.id,u.username,u.email,u.full_name,u.role,u.active,
                      u.last_login,u.location_id,
                      COALESCE(b.name,'All Locations') AS location_name,
                      CASE WHEN u.active THEN 'Active' ELSE 'Inactive' END AS status
               FROM users u
               LEFT JOIN locations b ON b.id=u.location_id
               WHERE u.location_id=%s ORDER BY u.username""",
            (location_id,),
        )
        stats = {
            "total": len(users),
            "active": sum(1 for u in users if u.get("active")),
            "pending": sum(1 for u in users if not u.get("active")),
            "locations": 1 if query_db("SELECT id FROM locations WHERE id=%s AND active=TRUE", (location_id,), one=True) else 0,
        }
        locations = query_db(
            "SELECT id,name FROM locations WHERE id=%s AND active=TRUE",
            (location_id,),
        )
        return render_template("settings_users.html", users=users, user_stats=stats, locations=locations)

    form_data = request.form
    action = form_data.get("action")
    if action == "invite":
        email = form_data.get("email","").strip().lower()
        if not email or "@" not in email:
            flash("A valid email address is required", "error")
            return redirect(url_for("settings.settings_users"))
        role = form_data.get("role","reception").strip().lower()
        valid_roles = {"owner","manager","reception","technician","readonly","admin"}
        if role not in valid_roles:
            flash("Invalid role specified", "error")
            return redirect(url_for("settings.settings_users"))
        if query_db("SELECT id FROM users WHERE lower(email)=lower(%s)", (email,), one=True):
            flash("A user with this email already exists", "error")
            return redirect(url_for("settings.settings_users"))
        import secrets
        from werkzeug.security import generate_password_hash
        temp_password = secrets.token_urlsafe(9)
        execute_db(
            """INSERT INTO users
               (owner_id,location_id,username,email,password,password_hash,full_name,role,active,must_reset_password,created_at,updated_at)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (user.get("owner_id"),location_id,email,email,"",generate_password_hash(temp_password),
             form_data.get("full_name","").strip(),role,1,1,utc_now(),utc_now()),
        )
        flash(f"Invitation prepared for {email}. Temporary password: {temp_password}", "info")
    elif action == "toggle_status":
        user_id = form_data.get("user_id")
        if user_id:
            row = query_db("SELECT active FROM users WHERE id=%s AND location_id=%s", (user_id,location_id), one=True)
            if row:
                execute_db("UPDATE users SET active=%s,updated_at=%s WHERE id=%s AND location_id=%s",
                           (0 if row["active"] else 1,utc_now(),user_id,location_id))
    elif action == "change_role":
        user_id = form_data.get("user_id")
        role = form_data.get("new_role","").strip().lower()
        if role not in {"owner","manager","reception","technician","readonly","admin"}:
            flash("Invalid role specified", "error")
        elif user_id:
            execute_db("UPDATE users SET role=%s,updated_at=%s WHERE id=%s AND location_id=%s",
                       (role,utc_now(),user_id,location_id))
    return redirect(url_for("settings.settings_users"))


@settings_bp.route("/settings/notifications", methods=["GET", "POST"])
@login_required
def settings_notifications():
    inactive_redirect = active_location_required()
    if inactive_redirect:
        return inactive_redirect
    location_id = current_user()["location_id"]
    import json
    defaults = {
        "email_enabled": True, "whatsapp_enabled": True, "sms_enabled": False,
        "booking_confirmation": True, "reminder_24h": True, "reminder_2h": False,
        "follow_up_24h": True, "review_request": True, "promotional_offers": False,
        "whatsapp_booking_reminder": True, "whatsapp_service_update": True,
        "whatsapp_promotional": False, "system_alerts_enabled": True,
        "alert_automation_failure": True, "alert_whatsapp_disconnect": True,
        "alert_low_inventory": False, "alert_system_update": True,
        "daily_summary": False, "notification_delivery_time": "08:00",
        "weekly_summary": False, "weekly_summary_day": "monday",
    }
    row = query_db("SELECT notification_preferences_json FROM locations WHERE id=%s", (location_id,), one=True) or {}
    try:
        saved = json.loads(row.get("notification_preferences_json") or "{}")
        if isinstance(saved, dict):
            defaults.update(saved)
    except (TypeError, ValueError):
        pass
    if request.method == "POST":
        for key in defaults:
            if key in {"notification_delivery_time","weekly_summary_day"}:
                defaults[key] = request.form.get(key, defaults[key]).strip()
            else:
                defaults[key] = request.form.get(key) == "on"
        execute_db(
            "UPDATE locations SET notification_preferences_json=%s,updated_at=%s WHERE id=%s",
            (json.dumps(defaults),utc_now(),location_id),
        )
        flash("Notification settings updated successfully", "success")
        return redirect(url_for("settings.settings_notifications"))
    return render_template("settings_notifications.html", notifications=defaults)


# WhatsApp settings
@settings_bp.route("/settings/whatsapp", methods=["GET", "POST"])
@login_required
def settings_whatsapp():
    """Canonical WhatsApp settings: Meta Embedded Signup connection screen."""
    inactive_redirect = active_location_required()
    if inactive_redirect:
        return inactive_redirect
    return render_template("connect_whatsapp.html", onboarding=False)

