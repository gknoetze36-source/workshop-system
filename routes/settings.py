import json
import logging
from datetime import datetime, timezone

from flask import Blueprint, render_template, request, redirect, url_for, flash, session, Response
from werkzeug.security import check_password_hash, generate_password_hash
from database import query_db, execute_db, utc_now
from helpers.audit import record_audit
from helpers.permission import require_role, ADMIN_ROLES
from services.export_service import export_to_json, ExportError
from helpers.security_events import (
    record_security_event, PASSWORD_CHANGED, PASSWORD_RESET_BY_ADMIN,
    ACCOUNT_DEACTIVATED, ACCOUNT_REACTIVATED,
)
from services.auth_service import login_required, active_location_required, current_user, bump_session_version

settings_bp = Blueprint("settings", __name__)

logger = logging.getLogger(__name__)

MIN_PASSWORD_LENGTH = 8


@settings_bp.route("/settings/password", methods=["GET", "POST"])
@login_required
def change_password():
    """Let a signed-in user change their own password.

    Deliberately NOT gated on active_location_required(): a user carrying a
    temporary password is redirected here from _enforce_session_state before
    they can reach anything else, including onboarding, so this page has to be
    reachable in that state.

    Changing the password revokes every other session for the account, then
    re-stamps this one so the user who just changed it stays signed in.
    """
    user = current_user()
    must_reset = bool(user.get("must_reset_password"))

    if request.method == "GET":
        return render_template("settings_password.html", must_reset=must_reset)

    current_password = request.form.get("current_password") or ""
    new_password = request.form.get("new_password") or ""
    confirm_password = request.form.get("confirm_password") or ""

    row = query_db("SELECT id, password_hash FROM users WHERE id=%s", (user.get("id"),), one=True)
    if not row:
        flash("Account not found.", "error")
        return redirect(url_for("auth.logout"))

    stored_hash = row.get("password_hash") or ""
    try:
        current_ok = bool(stored_hash and check_password_hash(stored_hash, current_password))
    except (ValueError, TypeError):
        current_ok = False

    if not current_ok:
        flash("Your current password is incorrect.", "error")
        return render_template("settings_password.html", must_reset=must_reset)
    if len(new_password) < MIN_PASSWORD_LENGTH:
        flash(f"New password must be at least {MIN_PASSWORD_LENGTH} characters.", "error")
        return render_template("settings_password.html", must_reset=must_reset)
    if new_password != confirm_password:
        flash("New passwords do not match.", "error")
        return render_template("settings_password.html", must_reset=must_reset)
    if new_password == current_password:
        flash("New password must be different from your current password.", "error")
        return render_template("settings_password.html", must_reset=must_reset)

    execute_db(
        "UPDATE users SET password_hash=%s, must_reset_password=%s, updated_at=%s WHERE id=%s",
        (generate_password_hash(new_password), False, utc_now(), user["id"]),
    )
    # Revoke every session for this account, then re-stamp the current one so
    # the person who just changed their password is not signed out.
    new_version = bump_session_version(user["id"])
    session_user = dict(user)
    session_user["session_version"] = new_version
    session_user["must_reset_password"] = False
    session["user"] = session_user

    record_audit(
        "user.password_changed", "user", entity_id=user["id"],
        actor_user=user, location_id=user.get("location_id"), user_id=user["id"],
        details={"self_service": True},
    )
    record_security_event(
        PASSWORD_CHANGED, user_id=user["id"], identifier=user.get("email"),
        identifier_is_known_account=True, location_id=user.get("location_id"),
        details={"self_service": True},
    )
    flash("Your password has been changed. Other sessions have been signed out.", "success")
    return redirect(url_for("settings.settings_overview"))



@settings_bp.route("/settings")
@settings_bp.route("/settings/")
@login_required
def settings_overview():
    """Settings hub.

    This previously redirected straight to /settings/business, which meant no
    other settings page was reachable from the interface at all -- including
    the password change and data export pages, whose routes existed but which
    nothing linked to. The hub lists the sections the signed-in user is
    actually allowed to open, so a reception user is not shown links that will
    bounce them back with an access-denied message.
    """
    user = current_user()
    role = (user.get("role") or "").strip().lower()
    is_admin = role in {"owner", "admin"} or role in {"super_admin", "phanta_admin", "platform_admin"}

    sections = [
        {"endpoint": "settings.settings_business", "label": "Business Information",
         "description": "Your business name, contact details and address.", "admin_only": True},
        {"endpoint": "settings.settings_hours", "label": "Operating Hours",
         "description": "When the workshop is open.", "admin_only": True},
        {"endpoint": "settings.settings_users", "label": "Users",
         "description": "Invite staff, change roles, and reset passwords.", "admin_only": True},
        {"endpoint": "settings.settings_notifications", "label": "Notifications",
         "description": "What PHANTA sends, and when.", "admin_only": True},
        # /settings/whatsapp existed but nothing linked to it, so the only way
        # to reach the Embedded Signup screen was to type the URL by hand.
        {"endpoint": "settings.settings_whatsapp", "label": "WhatsApp Connection",
         "description": "Connect or reconnect your WhatsApp Business Account.",
         "admin_only": True},
        {"endpoint": "settings.change_password", "label": "Change Password",
         "description": "Change your own password. Signs out your other devices.",
         "admin_only": False},
        {"endpoint": "settings.data_export", "label": "Export Your Data",
         "description": "Download a copy of your workshop's records.", "admin_only": True},
    ]
    visible = [s for s in sections if is_admin or not s["admin_only"]]
    return render_template("settings_overview.html", sections=visible)

# Business information settings
@settings_bp.route("/settings/business", methods=["GET", "POST"])
@login_required
def settings_business():
    inactive_redirect = active_location_required()
    if inactive_redirect:
        return inactive_redirect
    user = current_user()
    if user.get("role") not in {"owner", "admin"}:
        flash("Access denied. Administrator privileges required.", "error")
        return redirect(url_for("workshop_dashboard.workshop_dashboard"))
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
        # These form field names are checked to decide whether an empty
        # submission should explicitly clear the target column, so the
        # membership check must happen BEFORE aliasing renames `field` to
        # its target column -- checking after (the previous order) tested
        # the aliased name (e.g. "contact_phone") against a list that only
        # ever contains the original form field name
        # ("secondary_phone_number"), so an empty secondary_phone_number
        # submission silently failed to clear contact_phone.
        clearable_form_fields = [
            'trading_name', 'business_registration_number', 'vat_number',
            'secondary_phone_number', 'website', 'description',
        ]
        for field in fields + ["email", "primary_whatsapp_number", "secondary_phone_number"]:
            if field in form_data:
                value = form_data[field].strip()
                is_clearable = field in clearable_form_fields
                field = aliases.get(field, field)
                if value:  # Only update if not empty
                    updates[field] = value
                elif is_clearable:
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
    user = current_user()
    if user.get("role") not in {"owner", "admin", "manager"}:
        flash("Access denied. Manager privileges required.", "error")
        return redirect(url_for("workshop_dashboard.workshop_dashboard"))
    location_id = user["location_id"]
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
        return redirect(url_for("workshop_dashboard.workshop_dashboard"))

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
             form_data.get("full_name","").strip(),role,True,True,utc_now(),utc_now()),
        )
        flash(f"Invitation prepared for {email}. Temporary password: {temp_password}", "info")
    elif action == "toggle_status":
        user_id = form_data.get("user_id")
        if user_id:
            row = query_db("SELECT active FROM users WHERE id=%s AND location_id=%s", (user_id,location_id), one=True)
            if row:
                now_active = not row["active"]
                execute_db("UPDATE users SET active=%s,updated_at=%s WHERE id=%s AND location_id=%s",
                           (now_active,utc_now(),user_id,location_id))
                # Deactivating must take effect immediately, not whenever the
                # user's existing session happens to expire.
                if not now_active:
                    bump_session_version(user_id)
                record_audit(
                    "user.deactivated" if not now_active else "user.reactivated",
                    "user", entity_id=user_id, actor_user=user,
                    location_id=location_id, user_id=user_id,
                )
                record_security_event(
                    ACCOUNT_DEACTIVATED if not now_active else ACCOUNT_REACTIVATED,
                    user_id=user_id, location_id=location_id,
                    details={"actor_user_id": user.get("id")},
                )
    elif action == "reset_password":
        # No email provider exists in PHANTA, so password recovery is an
        # owner/admin action: a temporary password is generated, shown once to
        # the administrator to pass on out of band, and the user is forced to
        # change it at next login by must_reset_password.
        target_id = form_data.get("user_id")
        target = query_db(
            "SELECT id, email, username FROM users WHERE id=%s AND location_id=%s",
            (target_id, location_id), one=True,
        ) if target_id else None
        if not target:
            flash("User not found.", "error")
        else:
            import secrets
            from werkzeug.security import generate_password_hash as _hash
            temp_password = secrets.token_urlsafe(9)
            execute_db(
                "UPDATE users SET password_hash=%s, must_reset_password=%s, updated_at=%s WHERE id=%s AND location_id=%s",
                (_hash(temp_password), True, utc_now(), target_id, location_id),
            )
            # Any session the user (or an attacker) currently holds is dead.
            bump_session_version(target_id)
            record_audit(
                "user.password_reset_by_admin", "user", entity_id=target_id,
                actor_user=user, location_id=location_id, user_id=target_id,
            )
            record_security_event(
                PASSWORD_RESET_BY_ADMIN, user_id=target_id, location_id=location_id,
                identifier=target.get("email"), identifier_is_known_account=True,
                details={"actor_user_id": user.get("id")},
            )
            flash(
                f"Temporary password for {target.get('email') or target.get('username')}: {temp_password} "
                "-- give this to them directly. They must change it at next sign-in.",
                "info",
            )
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
    user = current_user()
    if user.get("role") not in {"owner", "admin", "manager"}:
        flash("Access denied. Manager privileges required.", "error")
        return redirect(url_for("workshop_dashboard.workshop_dashboard"))
    location_id = user["location_id"]
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
    user = current_user()
    if user.get("role") not in {"owner", "admin"}:
        flash("Access denied. Administrator privileges required.", "error")
        return redirect(url_for("workshop_dashboard.workshop_dashboard"))
    return render_template("connect_whatsapp.html", onboarding=False)



@settings_bp.route("/settings/export", methods=["GET", "POST"])
@login_required
@require_role(*ADMIN_ROLES)
def data_export():
    """Produce a copy of this workshop's data.

    Implements the pipeline the security plan requires:
      REQUEST -> AUTHORISATION -> TENANT-SCOPED DATA -> EXPORT
              -> SECURE DELIVERY -> AUDIT LOG

    AUTHORISATION: owner/admin only. An export is a complete copy of the
    workshop's customer records, so it is not something reception should be
    able to produce.

    TENANT SCOPE: the location comes from the authenticated session and is
    never read from the request. services/export_service.py filters every
    query on it, and PostgreSQL RLS enforces the same boundary independently.

    DELIVERY: streamed as a download rather than written to disk, so no export
    file is left sitting in the container filesystem or in a publicly
    reachable path.

    AUDIT: every export is recorded in both the tenant audit log and the
    platform security log, because a bulk extract of personal information is
    exactly the event a breach investigation needs to be able to find.
    """
    user = current_user()
    location_id = user.get("location_id")

    if request.method == "GET":
        return render_template("settings_export.html")

    try:
        payload = export_to_json(location_id)
        summary = json.loads(payload).get("counts", {})
    except ExportError:
        logger.exception("data_export_refused location_id=%s", location_id)
        flash("The export could not be produced. Please contact support.", "error")
        return redirect(url_for("settings.data_export"))

    record_audit(
        "data.exported", "location", entity_id=location_id,
        actor_user=user, location_id=location_id, user_id=user.get("id"),
        details={"counts": summary},
    )
    record_security_event(
        "privacy.data_exported",
        user_id=user.get("id"), location_id=location_id,
        details={"record_counts": summary},
    )

    stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    return Response(
        payload,
        mimetype="application/json",
        headers={
            "Content-Disposition": f'attachment; filename="phanta-export-{location_id}-{stamp}.json"',
            # An export is personal information; never let it sit in a cache.
            "Cache-Control": "no-store, no-cache, must-revalidate, private",
            "Pragma": "no-cache",
        },
    )
