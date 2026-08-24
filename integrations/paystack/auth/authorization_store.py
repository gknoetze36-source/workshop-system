"""Encrypted Paystack authorization (saved card) storage.

Paystack's recurring-charge model requires storing the authorization_code
returned on a customer's first successful payment, then passing it to
POST /transaction/charge_authorization to charge them again without
re-entering card details.

Paystack's own documentation is explicit that an authorization code is not
a card number but can still be used to move money, and should be treated
as a credential. This mirrors integrations/meta/auth/token_store.py's
approach exactly: Fernet encryption, key supplied via environment and never
persisted, so the ciphertext in payment_customers.authorization_secret_ref
is useless on its own.

Reuses META_TOKEN_ENCRYPTION_KEY by default so a deployment doesn't need a
second key to manage; set PAYSTACK_AUTH_ENCRYPTION_KEY to use a separate
one.
"""
from __future__ import annotations

import json
import os

from cryptography.fernet import Fernet, InvalidToken
from sqlalchemy.orm import Session

from models.integration_models import PaymentCustomer


class PaystackAuthorizationStore:
    ENV_KEY = "PAYSTACK_AUTH_ENCRYPTION_KEY"
    FALLBACK_ENV_KEY = "META_TOKEN_ENCRYPTION_KEY"

    def __init__(self, key: str | bytes | None = None):
        raw = key if key is not None else (
            os.getenv(self.ENV_KEY, "").strip() or os.getenv(self.FALLBACK_ENV_KEY, "").strip()
        )
        if isinstance(raw, str):
            raw = raw.encode()
        if not raw:
            raise RuntimeError(
                f"{self.ENV_KEY} (or {self.FALLBACK_ENV_KEY}) is required to store Paystack authorizations"
            )
        try:
            self._fernet = Fernet(raw)
        except (ValueError, TypeError) as exc:
            raise ValueError(
                f"{self.ENV_KEY} must be a valid Fernet key; generate one with Fernet.generate_key()"
            ) from exc

    def encrypt(self, payload: dict) -> str:
        if not payload or not payload.get("authorization_code"):
            raise ValueError("authorization payload must include an authorization_code")
        return self._fernet.encrypt(json.dumps(payload).encode()).decode()

    def decrypt(self, ciphertext: str) -> dict:
        if not ciphertext:
            raise ValueError("stored Paystack authorization is missing")
        try:
            return json.loads(self._fernet.decrypt(ciphertext.encode()).decode())
        except InvalidToken as exc:
            raise ValueError("Unable to decrypt Paystack authorization with the configured key") from exc

    def save_authorization(self, session: Session, location_id: int, email: str, authorization: dict):
        """Persist a reusable authorization for later recurring charges.

        Paystack marks non-reusable authorizations (bank transfer, USSD, and
        typically mobile money) with reusable=false -- storing those would
        guarantee a failed charge later, so they're rejected here rather
        than silently saved.

        The card metadata (last4, brand, expiry) is kept alongside the code
        because it's needed to show the customer which card is on file and
        to warn them before it expires. Paystack's docs recommend storing
        the whole authorization object for exactly this reason.
        """
        if not authorization or not authorization.get("authorization_code"):
            return None
        if not authorization.get("reusable"):
            return None

        customer = (
            session.query(PaymentCustomer)
            .filter_by(location_id=location_id, email=email)
            .one_or_none()
        )
        if customer is None:
            return None

        payload = {
            "authorization_code": authorization.get("authorization_code"),
            "last4": authorization.get("last4"),
            "brand": authorization.get("brand") or authorization.get("card_type"),
            "exp_month": authorization.get("exp_month"),
            "exp_year": authorization.get("exp_year"),
            "bank": authorization.get("bank"),
            "channel": authorization.get("channel"),
            "signature": authorization.get("signature"),
            # Paystack only allows charging an authorization with the same
            # email it was created against, so it's stored with the code.
            "email": email,
        }
        customer.authorization_secret_ref = self.encrypt(payload)
        session.flush()
        return customer

    def load_authorization(self, session: Session, location_id: int) -> dict | None:
        customer = (
            session.query(PaymentCustomer)
            .filter(
                PaymentCustomer.location_id == location_id,
                PaymentCustomer.authorization_secret_ref.isnot(None),
            )
            .first()
        )
        if customer is None or not customer.authorization_secret_ref:
            return None
        return self.decrypt(customer.authorization_secret_ref)
