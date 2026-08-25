"""Inbound WhatsApp messages and outbound delivery-status handlers."""
from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from models.core import Customer, Conversation, Message


class MetaMessageHandlers:
    def __init__(self, session: Session):
        self.session = session

    def inbound(self, *, location_id: int | None, phone_number_id: str, message: dict) -> dict:
        if location_id is None:
            return {"stored": False, "reason": "location_not_resolved"}

        sender = str(message.get("from", "")).strip()
        external_id = str(message.get("id", "")).strip()
        if not sender or not external_id:
            return {"stored": False, "reason": "missing_sender_or_message_id"}

        existing = self.session.scalar(
            select(Message).where(Message.channel == "whatsapp", Message.whatsapp_message_id == external_id)
        )
        if existing:
            return {"stored": True, "duplicate": True, "message_id": existing.id}

        normalized = "".join(ch for ch in sender if ch.isdigit())
        customer = self.session.scalar(
            select(Customer).where(
                Customer.location_id == location_id,
                Customer.whatsapp_number == normalized,
                Customer.deleted_at.is_(None),
            )
        )
        # Phase 12 owns reception identity. A first-time WhatsApp sender is
        # created with a neutral placeholder; the Service Advisor fills the
        # actual name through structured extraction in its next turn.
        if customer is None:
            customer = Customer(
                location_id=location_id,
                first_name="New",
                last_name="Customer",
                whatsapp_number=normalized,
            )
            self.session.add(customer)
            self.session.flush()

        conversation = self.session.scalar(
            select(Conversation)
            .where(Conversation.location_id == location_id, Conversation.customer_id == customer.id, Conversation.channel == "whatsapp")
            .order_by(Conversation.started_at.desc())
        )
        if conversation is None or conversation.ended_at is not None:
            conversation = Conversation(location_id=location_id, customer_id=customer.id, channel="whatsapp")
            self.session.add(conversation)
            self.session.flush()

        body = self._extract_body(message)
        obj = Message(
            location_id=location_id,
            conversation_id=conversation.id,
            direction="inbound",
            channel="whatsapp",
            body=body,
            whatsapp_message_id=external_id,
            status="received",
        )
        self.session.add(obj)
        self.session.flush()
        # Generic hook for any automation_rule configured against
        # "message.received" -- unlike booking.created/service.annual_due
        # etc., this isn't tied to a specific canned template; it's the
        # general-purpose "something arrived" trigger a client can build
        # their own condition/action against (matching how Zapier treats
        # "new message" as a plain, reusable trigger rather than a
        # single-purpose one). Only fires for a genuinely new message,
        # not a webhook redelivery of one already stored (see the
        # `existing` check above) -- webhook_events already guards
        # against the whole delivery being reprocessed, but this is a
        # second, cheaper guard specifically for automation firing.
        from services.automation_engine import fire_event
        fire_event("message.received", location_id, context={
            "customer_id": customer.id,
            "conversation_id": conversation.id,
            "message_id": obj.id,
            "body": body,
            "phone_number": normalized,
            "channel": "whatsapp",
        })
        return {
            "stored": True,
            "duplicate": False,
            "message_id": obj.id,
            "conversation_id": conversation.id,
            "customer_id": customer.id,
            "body": body,
            "phone_number": normalized,
        }

    def status(self, *, location_id: int | None, status: dict) -> dict:
        external_id = str(status.get("id", "")).strip()
        new_status = str(status.get("status", "")).strip().lower()
        if not external_id or not new_status:
            return {"updated": False, "reason": "missing_status_fields"}

        stmt = select(Message).where(Message.channel == "whatsapp", Message.whatsapp_message_id == external_id)
        if location_id is not None:
            stmt = stmt.where(Message.location_id == location_id)
        message = self.session.scalar(stmt)
        if message is None:
            return {"updated": False, "reason": "message_not_found", "wamid": external_id}
        message.status = new_status
        self.session.flush()
        return {"updated": True, "message_id": message.id, "status": new_status}

    @staticmethod
    def _extract_body(message: dict) -> str:
        message_type = message.get("type", "unknown")
        if message_type == "text":
            return str((message.get("text") or {}).get("body", ""))
        if message_type == "button":
            return str((message.get("button") or {}).get("text", ""))
        if message_type == "interactive":
            interactive = message.get("interactive") or {}
            return str(interactive.get("button_reply", {}).get("title") or interactive.get("list_reply", {}).get("title") or "[interactive message]")
        return f"[{message_type} message]"
