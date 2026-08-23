from __future__ import annotations
from helpers.location import current_location_id

from flask import Blueprint, jsonify, request, g, session
from database import get_session
from integrations.meta.auth.oauth_client import MetaOAuthClient
from integrations.meta.services.embedded_signup_service import EmbeddedSignupService
from integrations.meta.whatsapp.phone_number_service import PhoneNumberService, PhoneRegistrationError
meta_bp = Blueprint("meta", __name__, url_prefix="/integrations/meta")

@meta_bp.get("/embedded-signup/config")
def embedded_signup_config():
    try: current_location_id()
    except PermissionError as exc: return jsonify({"error": str(exc)}), 401
    public = MetaOAuthClient().public_configuration()
    return jsonify({"app_id": public.app_id, "config_id": public.embedded_signup_config_id, "graph_api_version": public.graph_api_version})

@meta_bp.post("/embedded-signup/start")
def embedded_signup_start():
    try: location_id = current_location_id()
    except PermissionError as exc: return jsonify({"error": str(exc)}), 401
    session = get_session()
    try:
        launch = EmbeddedSignupService().begin(session, location_id); session.commit()
        return jsonify({"app_id": launch.app_id, "config_id": launch.config_id, "graph_api_version": launch.graph_api_version, "state_nonce": launch.state_nonce, "expires_at": launch.expires_at.isoformat()})
    except Exception:
        session.rollback(); return jsonify({"error": "could not start Embedded Signup"}), 500
    finally: session.close()

@meta_bp.post("/embedded-signup/callback")
def embedded_signup_callback():
    try: location_id = current_location_id()
    except PermissionError as exc: return jsonify({"error": str(exc)}), 401
    payload = request.get_json(silent=True) or {}
    if not payload.get("code") or not payload.get("state_nonce"): return jsonify({"error": "code and state_nonce are required"}), 400
    session = get_session()
    try:
        result = EmbeddedSignupService().complete(session, location_id=location_id, state_nonce=payload["state_nonce"], code=payload["code"], business_id=payload.get("business_id"), waba_id=payload.get("waba_id"), phone_number_id=payload.get("phone_number_id")); session.commit()
        return jsonify({"status": "connected", "business_id": result.business_id, "waba_id": result.waba_id, "phone_number_id": result.phone_number_id, "token_type": result.token_type, "token_expires_in": result.expires_in})
    except ValueError as exc:
        session.rollback(); return jsonify({"error": str(exc)}), 400
    except Exception:
        session.rollback(); return jsonify({"error": "Embedded Signup callback failed"}), 500
    finally: session.close()

@meta_bp.get("/connection-health")
def meta_connection_health():
    try:
        location_id = current_location_id()
    except PermissionError as exc:
        return jsonify({"error": str(exc)}), 401
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
def register_phone():
    payload = request.get_json(silent=True) or {}
    return _phone_operation(lambda session, location_id: _registration_result(
        PhoneNumberService().register(session, location_id, payload.get("pin"))
    ))


@meta_bp.post("/phone/request-code")
def request_phone_code():
    payload = request.get_json(silent=True) or {}
    return _phone_operation(lambda session, location_id: _registration_result(
        PhoneNumberService().request_verification_code(
            session, location_id, code_method=payload.get("code_method"), language=payload.get("language")
        )
    ))


@meta_bp.post("/phone/verify-code")
def verify_phone_code():
    payload = request.get_json(silent=True) or {}
    return _phone_operation(lambda session, location_id: _registration_result(
        PhoneNumberService().verify_code(session, location_id, payload.get("code"))
    ))


@meta_bp.post("/phone/pin")
def set_phone_pin():
    payload = request.get_json(silent=True) or {}
    return _phone_operation(lambda session, location_id: _registration_result(
        PhoneNumberService().set_pin(session, location_id, payload.get("pin"))
    ))


@meta_bp.get("/phone/info")
def phone_info():
    return _phone_operation(lambda session, location_id: PhoneNumberService().phone_info(session, location_id))


@meta_bp.get("/phone/waba")
def waba_info():
    return _phone_operation(lambda session, location_id: PhoneNumberService().waba_info(session, location_id))
