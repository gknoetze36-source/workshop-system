"""Meta User Data Deletion callback support.

This deliberately deletes only data PHANTA can reliably associate with the
Meta app-scoped user ID supplied in Meta's signed_request. It does not guess
which workshop/customer record belongs to a Meta identity.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from models.core import AuditLog
from models.integration_models import MetaSocialConnection, MetaSocialOAuthSession


class InvalidSignedRequest(ValueError):
    """Raised when Meta's signed_request is malformed or fails verification."""


def _b64url_decode(value: str) -> bytes:
    value = value.strip()
    if not value:
        raise InvalidSignedRequest("empty signed request component")
    value += "=" * (-len(value) % 4)
    try:
        return base64.urlsafe_b64decode(value.encode("ascii"))
    except Exception as exc:  # binascii.Error differs by Python/runtime
        raise InvalidSignedRequest("invalid base64url encoding") from exc


def parse_signed_request(signed_request: str, app_secret: str) -> dict[str, Any]:
    """Verify and decode Meta's HMAC-SHA256 signed_request payload."""
    if not signed_request or not app_secret:
        raise InvalidSignedRequest("signed_request and app secret are required")

    parts = signed_request.split(".")
    if len(parts) != 2:
        raise InvalidSignedRequest("signed_request must contain signature and payload")
    encoded_sig, encoded_payload = parts

    signature = _b64url_decode(encoded_sig)
    expected = hmac.new(
        app_secret.encode("utf-8"),
        encoded_payload.encode("ascii"),
        hashlib.sha256,
    ).digest()
    if not hmac.compare_digest(signature, expected):
        raise InvalidSignedRequest("signed_request signature mismatch")

    try:
        payload = json.loads(_b64url_decode(encoded_payload).decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise InvalidSignedRequest("signed_request payload is not valid JSON") from exc

    if not isinstance(payload, dict):
        raise InvalidSignedRequest("signed_request payload must be an object")
    if payload.get("algorithm") != "HMAC-SHA256":
        raise InvalidSignedRequest("unsupported signed_request algorithm")
    if not payload.get("user_id"):
        raise InvalidSignedRequest("signed_request does not contain user_id")
    return payload



def find_meta_user_locations(session: Session, meta_user_id: str) -> list[int]:
    """Resolve locations using the dedicated platform SELECT context."""
    return sorted(set(session.scalars(
        select(MetaSocialConnection.location_id).where(
            MetaSocialConnection.meta_user_id == str(meta_user_id)
        )
    ).all()))


def delete_meta_user_data(session: Session, meta_user_id: str) -> dict[str, int | bool]:
    """Remove Meta social data reliably linked to one Meta app-scoped user.

    The Flyer Lady connection is the PHANTA component that receives and stores
    a Meta user access token. WhatsApp business/system-user credentials are
    business assets, not the callback user's personal Facebook data, so they
    are intentionally not deleted here.
    """
    connections = session.scalars(
        select(MetaSocialConnection).where(
            MetaSocialConnection.meta_user_id == str(meta_user_id)
        )
    ).all()

    if not connections:
        return {"user_found": False, "connections_removed": 0, "oauth_sessions_removed": 0}

    location_ids = sorted({connection.location_id for connection in connections})
    oauth_sessions = session.scalars(
        select(MetaSocialOAuthSession).where(
            MetaSocialOAuthSession.location_id.in_(location_ids)
        )
    ).all()

    for connection in connections:
        session.add(
            AuditLog(
                location_id=connection.location_id,
                # S29: the audit trail must say WHICH Meta App caused the
                # event. A generic "meta_data_deletion_callback" actor and
                # "meta_user_data_deleted" action could not distinguish a
                # Flyer Lady deletion from a WhatsApp one once the two Apps
                # are separate.
                actor="meta_flyer_lady_data_deletion_callback",
                action="meta_flyer_lady_data_deletion",
                entity_type="MetaSocialConnection",
                entity_id=str(connection.id),
                after={"meta_user_data_removed": True},
            )
        )
        session.delete(connection)

    for oauth_session in oauth_sessions:
        session.delete(oauth_session)

    session.flush()
    return {
        "user_found": True,
        "connections_removed": len(connections),
        "oauth_sessions_removed": len(oauth_sessions),
    }


def deauthorize_flyer_lady_user(session: Session, meta_user_id: str) -> dict[str, int | bool]:
    """Revoke one Meta user's Flyer Lady authorization (S27, S28).

    Deauthorization is NOT deletion. When a user removes the Flyer Lady app
    from their Facebook account, the authorization is gone, so the stored
    Page/user tokens are useless and must not be retained -- but the
    workshop's own Flyer Lady content (specials, posts, click history) is
    VANTA business data, not the user's personal Facebook data, and is
    deliberately kept. A user who reconnects should find their specials
    intact.

    S28 boundary -- this MUST NOT touch any of:
      * WABA, WhatsApp phone numbers, WhatsApp System User credentials
      * WhatsApp billing or credit-line state
      * WhatsApp message history
    Those belong to the WhatsApp Meta App and a different authorization
    entirely. This function only ever queries MetaSocialConnection and
    MetaSocialOAuthSession, which makes that boundary structural rather
    than a matter of care.
    """
    connections = session.scalars(
        select(MetaSocialConnection).where(
            MetaSocialConnection.meta_user_id == str(meta_user_id)
        )
    ).all()

    if not connections:
        # Idempotent: a repeated or unknown deauthorization is a success,
        # not an error. Meta may retry.
        return {"user_found": False, "connections_revoked": 0, "oauth_sessions_removed": 0}

    location_ids = sorted({connection.location_id for connection in connections})
    oauth_sessions = session.scalars(
        select(MetaSocialOAuthSession).where(
            MetaSocialOAuthSession.location_id.in_(location_ids)
        )
    ).all()

    for connection in connections:
        session.add(
            AuditLog(
                location_id=connection.location_id,
                actor="meta_flyer_lady_deauthorization_callback",
                action="meta_flyer_lady_deauthorization",
                entity_type="MetaSocialConnection",
                entity_id=str(connection.id),
                after={"flyer_lady_authorization_revoked": True},
            )
        )
        # Clear the credentials, keep the row. The workshop's connection
        # history stays visible in the dashboard as "disconnected" rather
        # than silently vanishing, and no unusable token is retained.
        connection.encrypted_page_access_token = ""
        connection.connection_status = "revoked"

    for oauth_session in oauth_sessions:
        session.delete(oauth_session)

    return {
        "user_found": True,
        "connections_revoked": len(connections),
        "oauth_sessions_removed": len(oauth_sessions),
    }
