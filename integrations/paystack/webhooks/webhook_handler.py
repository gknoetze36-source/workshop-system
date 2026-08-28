from __future__ import annotations

import hashlib
import os
from datetime import datetime, timezone

from integrations.paystack.webhooks.signature_verifier import verify_signature
from integrations.paystack.repositories.webhook_event_repo import PaystackWebhookEventRepository
from integrations.paystack.webhooks.event_handlers.charge_handlers import handle_charge_success, handle_charge_failed
from integrations.paystack.webhooks.event_handlers.subscription_handlers import handle_subscription_create, handle_subscription_disable, handle_subscription_not_renew, handle_expiring_cards
from integrations.paystack.webhooks.event_handlers.invoice_handlers import handle_invoice
# Imported under an explicit name. It was imported as a bare `handle`, which
# is the same name as WebhookHandler.handle below. That resolves correctly --
# a method name is not in scope inside the method body, so the call reaches
# the module-level import -- but it reads as though the method calls itself,
# and any future refactor that adds a module-level `handle` would silently
# change which function runs.
from integrations.paystack.webhooks.event_handlers.dispute_handlers import (
    handle as handle_dispute_created,
)
from models.integration_models import Payment, PaymentCustomer, Subscription
from database import set_location_id


class PaystackWebhookRejected(Exception):
    pass


class WebhookHandler:
    def __init__(self, *, repository=None):
        self.repository = repository or PaystackWebhookEventRepository()

    def _resolve_location(self, session, data: dict, explicit_location_id=None):
        if explicit_location_id is not None:
            return int(explicit_location_id)
        from integrations.paystack.webhooks.webhook_location_resolver import resolve_paystack_location
        return resolve_paystack_location(session, data)

    def handle(self, session, raw_body: bytes, signature: str | None, payload: dict, location_id: int | None = None):
        secret = os.getenv("PAYSTACK_SECRET_KEY", "")
        if not secret or not verify_signature(raw_body, signature, secret):
            raise PaystackWebhookRejected("invalid Paystack webhook signature")

        event_type = payload.get("event")
        if not event_type:
            raise PaystackWebhookRejected("missing Paystack event type")
        data = payload.get("data") or {}
        resolved_location = self._resolve_location(session, data, location_id)
        if resolved_location is not None:
            set_location_id(session, resolved_location)
        if resolved_location is None:
            raise PaystackWebhookRejected("cannot resolve PHANTA location for Paystack event")

        # Paystack has no universal event-id field. Hashing the exact raw body
        # makes identical deliveries idempotent while allowing later invoice
        # updates for the same invoice to be processed.
        fingerprint = hashlib.sha256(raw_body).hexdigest()
        reference = data.get("reference") or data.get("subscription_code") or data.get("id") or "unknown"
        event_key = f"{event_type}:{reference}:{fingerprint}"
        existing = self.repository.get(session, event_key)
        if existing:
            return existing, False

        event = self.repository.create(
            session,
            location_id=resolved_location,
            event_key=event_key,
            event_type=event_type,
            payload=payload,
            signature_valid=True,
        )

        if event_type == "charge.success":
            handle_charge_success(session, data, resolved_location)
        elif event_type == "charge.failed":
            handle_charge_failed(session, data, resolved_location)
        elif event_type == "subscription.create":
            handle_subscription_create(session, data, resolved_location)
        elif event_type == "subscription.disable":
            handle_subscription_disable(session, data, resolved_location)
        elif event_type == "subscription.not_renew":
            handle_subscription_not_renew(session, data, resolved_location)
        elif event_type == "subscription.expiring_cards":
            handle_expiring_cards(session, data, resolved_location)
        elif event_type in {"invoice.create", "invoice.payment_failed", "invoice.update"}:
            handle_invoice(session, event_type, data, resolved_location)
        elif event_type == "refund.processed":
            from integrations.paystack.webhooks.event_handlers.refund_handlers import handle_refund_processed
            handle_refund_processed(session, data, resolved_location)
        elif event_type == "charge.dispute.create":
            handle_dispute_created(session, data, resolved_location)

        event.processing_status = "processed"
        event.processed_at = datetime.now(timezone.utc)
        return event, True
