"""Meta webhook envelope parsing, idempotency and field dispatch."""
from __future__ import annotations

import hashlib
import json
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from models.integration_models import MetaBusinessConnection
from .event_handlers.message_handlers import MetaMessageHandlers
from .event_handlers.quality_handlers import MetaQualityHandlers
from .event_handlers.template_handlers import MetaTemplateHandlers
from .event_handlers.account_handlers import MetaAccountHandlers
from ..repositories.connection_repo import MetaConnectionRepository
from ..repositories.webhook_event_repo import MetaWebhookEventRepository


class MetaWebhookRouter:
    def __init__(self, session: Session):
        self.session = session
        self.events = MetaWebhookEventRepository()
        self.connections = MetaConnectionRepository()
        self.message_handlers = MetaMessageHandlers(session)
        self.quality_handlers = MetaQualityHandlers(session)
        self.template_handlers = MetaTemplateHandlers(session)
        self.account_handlers = MetaAccountHandlers(session)

    def dispatch(self, payload: dict[str, Any], *, signature_valid: bool = True) -> dict[str, Any]:
        if payload.get("object") != "whatsapp_business_account":
            return {"accepted": False, "reason": "unsupported_object"}

        results: list[dict[str, Any]] = []
        for entry in payload.get("entry", []):
            waba_id = str(entry.get("id", "")) or None
            for change in entry.get("changes", []):
                field = str(change.get("field", ""))
                value = change.get("value") or {}
                phone_number_id = self._phone_number_id(value)
                location_id = self._resolve_location(waba_id, phone_number_id)
                if location_id is None:
                    # Never create/process location-owned records without a
                    # proven Location mapping.
                    continue
                for event_type, item in self._items_for_field(field, value):
                    external_id = self._event_id(event_type, waba_id, phone_number_id, item)
                    event, is_new = self.events.record(
                        self.session,
                        location_id=location_id,
                        waba_id=waba_id,
                        phone_number_id=phone_number_id,
                        external_event_id=external_id,
                        event_type=event_type,
                        payload=item,
                        signature_valid=signature_valid,
                    )
                    if not is_new:
                        results.append({"event_id": external_id, "duplicate": True, "event_type": event_type, "location_id": location_id})
                        continue
                    try:
                        result = self._handle(field, event_type, location_id, phone_number_id, value, item)
                        self.events.mark_processed(self.session, event)
                        results.append({"event_id": external_id, "duplicate": False, "event_type": event_type, "result": result, "location_id": location_id})
                    except Exception:
                        self.events.mark_failed(self.session, event)
                        raise
        return {"accepted": True, "results": results}

    def _resolve_location(self, waba_id: str | None, phone_number_id: str | None) -> int | None:
        matches: set[int] = set()
        if phone_number_id:
            connection = self.session.scalar(
                select(MetaBusinessConnection).where(
                    MetaBusinessConnection.phone_number_id == phone_number_id
                )
            )
            if connection:
                matches.add(int(connection.location_id))
        if waba_id:
            connection = self.session.scalar(
                select(MetaBusinessConnection).where(
                    MetaBusinessConnection.waba_id == waba_id
                )
            )
            if connection:
                matches.add(int(connection.location_id))
        return next(iter(matches)) if len(matches) == 1 else None

    @staticmethod
    def _phone_number_id(value: dict) -> str | None:
        metadata = value.get("metadata") or {}
        return str(metadata.get("phone_number_id")) if metadata.get("phone_number_id") else None

    @staticmethod
    def _items_for_field(field: str, value: dict):
        if field == "messages":
            messages = value.get("messages") or []
            statuses = value.get("statuses") or []
            for item in messages:
                yield "inbound_message", item
            for item in statuses:
                yield "message_status", item
            return
        if field == "account_update":
            yield "account_update", value
            return
        if field == "message_template_status_update":
            yield "message_template_status_update", value
            return
        if field == "phone_number_quality_update":
            yield "phone_number_quality_update", value
            return
        if field == "phone_number_name_update":
            yield "phone_number_name_update", value
            return
        if field == "security":
            yield "security", value
            return
        yield field or "unknown", value

    def _handle(self, field: str, event_type: str, location_id: int | None, phone_number_id: str | None, value: dict, item: dict) -> dict:
        if event_type == "inbound_message":
            return self.message_handlers.inbound(location_id=location_id, phone_number_id=phone_number_id or "", message=item)
        if event_type == "message_status":
            return self.message_handlers.status(location_id=location_id, status=item)
        if event_type == "message_template_status_update":
            return self.template_handlers.handle(location_id=location_id, payload=item)
        if event_type == "phone_number_quality_update":
            return self.quality_handlers.handle(
                location_id=location_id,
                phone_number=item.get("phone_number"),
                current_limit=item.get("current_limit"),
                event=item.get("event"),
            )
        if event_type == "account_update":
            return self.account_handlers.handle(location_id=location_id, value=item)
        # Name/security changes are still durable via webhook_events; no silent loss.
        return {"recorded": True, "handler": event_type}

    @staticmethod
    def _event_id(event_type: str, waba_id: str | None, phone_number_id: str | None, item: dict) -> str:
        natural_id = item.get("id") or item.get("message_template_id") or item.get("phone_number")
        if natural_id:
            return f"{event_type}:{natural_id}"
        canonical = json.dumps({"waba_id": waba_id, "phone_number_id": phone_number_id, "event_type": event_type, "item": item}, sort_keys=True, separators=(",", ":"), default=str).encode()
        return f"{event_type}:sha256:{hashlib.sha256(canonical).hexdigest()}"
