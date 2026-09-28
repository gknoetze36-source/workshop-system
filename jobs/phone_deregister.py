"""TEMPORARY one-off: deregister a WhatsApp number from the Cloud API.

Runs only while META_DEREGISTER_PHONE_NUMBER_ID is set on Railway. Used once
to free +27 82 417 0335 for Embedded Signup; remove this file and the
variable afterwards. Uses the System User token from the environment and
never logs it.
"""
from __future__ import annotations

import os


def run_phone_deregister() -> dict:
    phone_id = os.getenv("META_DEREGISTER_PHONE_NUMBER_ID", "").strip()
    if not phone_id:
        return {"skipped": True}
    if not phone_id.isdigit():
        return {"ok": False, "error": "META_DEREGISTER_PHONE_NUMBER_ID must be numeric"}

    from integrations.meta.auth.capability_config import WhatsAppMetaConfig
    from integrations.meta.business.provider_config import MetaProviderConfig
    from integrations.meta.services.graph_api_client import GraphApiClient

    token = MetaProviderConfig.from_env().system_user_token
    try:
        result = GraphApiClient(WhatsAppMetaConfig.from_env()).post_with_token(token, f"/{phone_id}/deregister")
        return {"ok": True, "phone_number_id": phone_id, "meta": result}
    except Exception as exc:  # report Meta's reason, never the token
        return {"ok": False, "phone_number_id": phone_id, "error": str(exc).replace(token, "[redacted]") if token else str(exc)}
