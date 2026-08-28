import os

from database import execute_db, fetch_one
from integrations.meta.auth.token_store import MetaTokenStore
from validators.phone_validator import normalize_phone


def encrypt_access_token(token):
    """Encrypt a Meta access token with the canonical Phase 6 key."""
    text = str(token or "")
    if not text:
        return text
    if text.startswith("enc:"):
        raise ValueError("Legacy encrypted Meta token format is no longer accepted; reconnect WhatsApp.")
    return MetaTokenStore().encrypt(text)


def decrypt_access_token(token):
    """Decrypt a Meta access token using META_TOKEN_ENCRYPTION_KEY."""
    text = str(token or "")
    if not text or text.startswith("enc:"):
        raise RuntimeError("Legacy Meta access-token encryption format is no longer supported.")
    return MetaTokenStore().decrypt(text)


def validate_messaging_account(account):
    if not account:
        return
    if account.get("provider") == "meta" and account.get("is_active"):
        if not account.get("location_id"):
            raise RuntimeError("Active Meta messaging account must belong to a location.")
        if not account.get("phone_number_id"):
            raise RuntimeError("Active Meta messaging account requires phone_number_id.")


def active_messaging_account(context=None, provider=None, channel="whatsapp", phone_number_id=None):
    """Return the canonical Meta WhatsApp connection for one Location.

    This compatibility adapter keeps the older service-layer call signature,
    but no longer reads the legacy messaging_accounts/workshop_id hierarchy.
    Secrets are decrypted only for the outbound provider call.
    """
    context = context or {}
    location_id = context.get("location_id")
    if not location_id:
        return None
    if provider not in (None, "meta") or channel != "whatsapp":
        return None

    from database import session_scope
    from sqlalchemy import select
    from models.integration_models import MetaBusinessConnection

    # This used to open get_platform_session(), which sets app.platform_admin
    # and disables location isolation at the database layer -- for an ordinary
    # outbound-message lookup. The SQL filtered on location_id, so no leak was
    # demonstrated, but the RLS guarantee was switched off during a routine
    # operation. A location-scoped session gives the same result with the
    # database still enforcing isolation, so a future change to this query
    # cannot reach another workshop's connection.
    with session_scope(location_id=int(location_id)) as db:
        stmt = select(MetaBusinessConnection).where(
            MetaBusinessConnection.location_id == int(location_id),
            MetaBusinessConnection.connection_status.in_(("connected", "expiring_soon")),
        )
        if phone_number_id:
            stmt = stmt.where(MetaBusinessConnection.phone_number_id == str(phone_number_id))
        connection = db.scalar(stmt)
        if connection is None:
            return None

        token = ""
        if connection.encrypted_access_token:
            token = MetaTokenStore().get_customer_token(connection)

        account = {
            "id": connection.id,
            "location_id": connection.location_id,
            "provider": "meta",
            "channel": "whatsapp",
            "account_id": connection.business_id,
            "business_account_id": connection.business_id,
            "whatsapp_business_account_id": connection.waba_id,
            "phone_number_id": connection.phone_number_id,
            "access_token": token,
            "is_active": True,
        }
        validate_messaging_account(account)
        return account



def messaging_configured(context=None, provider=None):
    return bool(active_messaging_account(context, provider=provider))



class MetaCloudApiProvider:
    provider = "meta"

    def send_text(self, account, recipient, body):
        import requests

        token = decrypt_access_token((account or {}).get("access_token") or "")
        phone_number_id = (account or {}).get("phone_number_id") or (account or {}).get("sender_id") or ""
        if not token:
            raise RuntimeError("Meta access token is not configured for this location.")
        if not phone_number_id:
            raise RuntimeError("Meta phone_number_id is not configured for this location.")

        response = requests.post(
            f"https://graph.facebook.com/v20.0/{phone_number_id}/messages",
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            json={
                "messaging_product": "whatsapp",
                "to": normalize_phone(recipient),
                "type": "text",
                "text": {"body": body},
            },
            timeout=20,
        )
        response.raise_for_status()
        return response.json()


PROVIDER_ADAPTERS = {
    "meta": MetaCloudApiProvider(),
}


def provider_adapter(provider):
    adapter = PROVIDER_ADAPTERS.get(provider)
    if not adapter:
        raise RuntimeError(f"Messaging provider is not supported: {provider}")
    return adapter



def send_provider_message(recipient, body, context=None, account=None):
    account = account or active_messaging_account(context)
    if not account:
        raise RuntimeError("No active messaging account is configured for this location.")
    return provider_adapter(account.get("provider")).send_text(account, recipient, body)
