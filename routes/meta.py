from __future__ import annotations
from services.integration_status import require_configured
from helpers.permission import require_role, ADMIN_ROLES
from helpers.location import current_location_id

from flask import Blueprint, jsonify, request, g, session
from database import get_session
from integrations.meta.auth.oauth_client import MetaOAuthClient
from integrations.meta.services.embedded_signup_service import EmbeddedSignupService
from integrations.meta.whatsapp.phone_number_service import PhoneNumberService, PhoneRegistrationError
meta_bp = Blueprint("meta", __name__, url_prefix="/integrations/meta")

@meta_bp.get("/embedded-signup/config")
@require_role(*ADMIN_ROLES)
def embedded_signup_config():
    try: current_location_id()
    except PermissionError as exc: return jsonify({"error": str(exc)}), 401
    # Without this guard, MetaOAuthClient() raises RuntimeError on a deployment
    # that has no Meta credentials, and the caller sees an unhandled 500 that
    # looks like a broken integration rather than an unconfigured one.
    unconfigured = require_configured("embedded_signup")
    if unconfigured: return jsonify(unconfigured[0]), unconfigured[1]
    public = MetaOAuthClient().public_configuration()
    return jsonify({"app_id": public.app_id, "config_id": public.embedded_signup_config_id, "graph_api_version": public.graph_api_version})

@meta_bp.post("/embedded-signup/start")
@require_role(*ADMIN_ROLES)
def embedded_signup_start():
    try: location_id = current_location_id()
    except PermissionError as exc: return jsonify({"error": str(exc)}), 401
    unconfigured = require_configured("embedded_signup")
    if unconfigured: return jsonify(unconfigured[0]), unconfigured[1]
    session = get_session()
    try:
        launch = EmbeddedSignupService().begin(session, location_id); session.commit()
        return jsonify({"app_id": launch.app_id, "config_id": launch.config_id, "graph_api_version": launch.graph_api_version, "state_nonce": launch.state_nonce, "expires_at": launch.expires_at.isoformat()})
    except Exception:
        session.rollback(); return jsonify({"error": "could not start Embedded Signup"}), 500
    finally: session.close()

@meta_bp.post("/embedded-signup/callback")
@require_role(*ADMIN_ROLES)
def embedded_signup_callback():
    try: location_id = current_location_id()
    except PermissionError as exc: return jsonify({"error": str(exc)}), 401
    payload = request.get_json(silent=True) or {}
    if not payload.get("code") or not payload.get("state_nonce"): return jsonify({"error": "code and state_nonce are required"}), 400
    unconfigured = require_configured("embedded_signup")
    if unconfigured: return jsonify(unconfigured[0]), unconfigured[1]
    session = get_session()
    try:
        result = EmbeddedSignupService().complete(
            session,
            location_id=location_id,
            state_nonce=payload["state_nonce"],
            code=payload["code"],
            business_id=payload.get("business_id"),
            waba_id=payload.get("waba_id"),
            phone_number_id=payload.get("phone_number_id"),
        )
        # Embedded Signup succeeding does not mean the workshop is connected.
        # Commit the exchanged token first so it is never lost if the
        # onboarding run below fails, then complete Meta Tech Provider
        # onboarding. The connection only becomes 'connected' inside that run.
        session.commit()
    except ValueError as exc:
        session.rollback(); session.close(); return jsonify({"error": str(exc)}), 400
    except Exception:
        session.rollback(); session.close(); return jsonify({"error": "Embedded Signup callback failed"}), 500

    try:
        run = _run_onboarding(session, location_id, phone_pin=payload.get("pin"))
        session.commit()
    except Exception:
        session.rollback()
        run = None
    finally:
        session.close()

    body = {
        "status": "onboarding",
        "business_id": result.business_id,
        "waba_id": result.waba_id,
        "phone_number_id": result.phone_number_id,
        "token_type": result.token_type,
        "token_expires_in": result.expires_in,
        "onboarding": run.as_dict() if run is not None else {
            "completed": False,
            "error": {"message": "Meta onboarding could not be started. Retry from the WhatsApp settings page."},
        },
    }
    if run is not None and run.completed:
        body["status"] = "connected"
    return jsonify(body)


def _run_onboarding(session, location_id: int, *, phone_pin=None):
    from integrations.meta.services.tech_provider_onboarding_service import (
        TechProviderOnboardingService,
    )

    pin = str(phone_pin).strip() if phone_pin else None
    return TechProviderOnboardingService().run_onboarding(
        session, location_id, phone_pin=pin or None
    )


@meta_bp.get("/onboarding/status")
@require_role(*ADMIN_ROLES)
def onboarding_status():
    """Client-safe onboarding progress. Never reports a partial run as connected."""
    try:
        location_id = current_location_id()
    except PermissionError as exc:
        return jsonify({"error": str(exc)}), 401
    unconfigured = require_configured("meta_app")
    if unconfigured:
        return jsonify(unconfigured[0]), unconfigured[1]
    session = get_session()
    try:
        from integrations.meta.services.tech_provider_onboarding_service import (
            TechProviderOnboardingService,
        )

        return jsonify(TechProviderOnboardingService().onboarding_status(session, location_id))
    except Exception:
        return jsonify({"error": "Could not read WhatsApp onboarding status"}), 500
    finally:
        session.close()


@meta_bp.post("/onboarding/resume")
@require_role(*ADMIN_ROLES)
def onboarding_resume():
    """Continue a partially completed onboarding from where it stopped.

    Accepts an optional 6-digit ``pin`` for the phone-registration step. The
    PIN is used for this request only: it is never persisted or logged.
    """
    try:
        location_id = current_location_id()
    except PermissionError as exc:
        return jsonify({"error": str(exc)}), 401
    unconfigured = require_configured("meta_system_user")
    if unconfigured:
        return jsonify(unconfigured[0]), unconfigured[1]
    payload = request.get_json(silent=True) or {}
    session = get_session()
    try:
        run = _run_onboarding(session, location_id, phone_pin=payload.get("pin"))
        session.commit()
        return jsonify(run.as_dict())
    except Exception:
        session.rollback()
        return jsonify({"error": "Meta onboarding could not be resumed"}), 502
    finally:
        session.close()


@meta_bp.get("/onboarding/diagnostics")
@require_role(*ADMIN_ROLES)
def onboarding_diagnostics():
    """Detailed but secret-free onboarding state for operators."""
    try:
        location_id = current_location_id()
    except PermissionError as exc:
        return jsonify({"error": str(exc)}), 401
    unconfigured = require_configured("meta_app")
    if unconfigured:
        return jsonify(unconfigured[0]), unconfigured[1]
    session = get_session()
    try:
        from integrations.meta.services.tech_provider_onboarding_service import (
            TechProviderOnboardingService,
        )

        return jsonify(TechProviderOnboardingService().diagnostics(session, location_id))
    except Exception:
        return jsonify({"error": "Could not read Meta onboarding diagnostics"}), 500
    finally:
        session.close()

@meta_bp.get("/connection-health")
@require_role(*ADMIN_ROLES)
def meta_connection_health():
    try:
        location_id = current_location_id()
    except PermissionError as exc:
        return jsonify({"error": str(exc)}), 401
    unconfigured = require_configured("meta_app")
    if unconfigured:
        return jsonify(unconfigured[0]), unconfigured[1]
    session = get_session()
    try:
        from integrations.meta.services.token_status_service import MetaTokenStatusService
        health = MetaTokenStatusService().check_connection(session, location_id)
        session.commit()
        return jsonify({
            "location_id": health.location_id,
            "connection_id": health.connection_id,
            "status": health.status,
            "healthy": health.healthy,
            "reconnect_required": health.reconnect_required,
            "token_valid": health.token_valid,
            "expires_at": health.expires_at.isoformat() if health.expires_at else None,
            "expires_in_seconds": health.expires_in_seconds,
            "permissions": list(health.permissions),
            "granular_scopes": list(health.granular_scopes),
            "checked_at": health.checked_at.isoformat(),
            "error": health.error,
        })
    except Exception:
        session.rollback()
        return jsonify({"error": "Meta connection health check failed"}), 502
    finally:
        session.close()


def _phone_operation(action):
    try:
        location_id = current_location_id()
    except PermissionError as exc:
        return jsonify({"error": str(exc)}), 401
    # Report an unconfigured deployment as such. Without this the call falls
    # through to the generic handler and returns 502, which says "the upstream
    # provider failed" when in fact PHANTA never had the credentials to call it.
    unconfigured = require_configured("whatsapp")
    if unconfigured:
        return jsonify(unconfigured[0]), unconfigured[1]
    session = get_session()
    try:
        result = action(session, location_id)
        session.commit()
        return jsonify(result)
    except PhoneRegistrationError as exc:
        session.rollback()
        return jsonify({"error": str(exc)}), 400
    except Exception:
        session.rollback()
        return jsonify({"error": "Meta phone registration operation failed"}), 502
    finally:
        session.close()


def _registration_result(result):
    return {
        "location_id": result.location_id,
        "phone_number_id": result.phone_number_id,
        "status": result.status,
        "success": result.success,
        "message": result.message,
    }


@meta_bp.post("/phone/register")
@require_role(*ADMIN_ROLES)
def register_phone():
    payload = request.get_json(silent=True) or {}
    return _phone_operation(lambda session, location_id: _registration_result(
        PhoneNumberService().register(session, location_id, payload.get("pin"))
    ))


@meta_bp.post("/phone/request-code")
@require_role(*ADMIN_ROLES)
def request_phone_code():
    payload = request.get_json(silent=True) or {}
    return _phone_operation(lambda session, location_id: _registration_result(
        PhoneNumberService().request_verification_code(
            session, location_id, code_method=payload.get("code_method"), language=payload.get("language")
        )
    ))


@meta_bp.post("/phone/verify-code")
@require_role(*ADMIN_ROLES)
def verify_phone_code():
    payload = request.get_json(silent=True) or {}
    return _phone_operation(lambda session, location_id: _registration_result(
        PhoneNumberService().verify_code(session, location_id, payload.get("code"))
    ))


@meta_bp.post("/phone/pin")
@require_role(*ADMIN_ROLES)
def set_phone_pin():
    payload = request.get_json(silent=True) or {}
    return _phone_operation(lambda session, location_id: _registration_result(
        PhoneNumberService().set_pin(session, location_id, payload.get("pin"))
    ))


@meta_bp.get("/phone/info")
@require_role(*ADMIN_ROLES)
def phone_info():
    return _phone_operation(lambda session, location_id: PhoneNumberService().phone_info(session, location_id))


@meta_bp.get("/phone/waba")
@require_role(*ADMIN_ROLES)
def waba_info():
    return _phone_operation(lambda session, location_id: PhoneNumberService().waba_info(session, location_id))
