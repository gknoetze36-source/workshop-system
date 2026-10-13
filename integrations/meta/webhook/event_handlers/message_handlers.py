"""Inbound WhatsApp messages and outbound delivery-status handlers."""
from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.orm import Session

from models.core import Customer, Conversation, Message, Task


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
        customer, conversation = self._chat_for(location_id, normalized)

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

    def _chat_for(self, location_id: int, normalized: str, *, name: str | None = None) -> tuple[Customer, Conversation]:
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
            first, _, last = (name or "").strip().partition(" ")
            customer = Customer(
                location_id=location_id,
                first_name=first or "New",
                last_name=last or ("" if first else "Customer"),
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
        return customer, conversation

    def _store(self, location_id: int, number: str, message: dict, *, direction: str, status: str) -> dict:
        """Store a message the workshop's WhatsApp Business app sent or already had (coexistence)."""
        external_id = str(message.get("id", "")).strip()
        normalized = "".join(ch for ch in str(number) if ch.isdigit())
        if not external_id or not normalized:
            return {"stored": False, "reason": "missing_number_or_message_id"}
        existing = self.session.scalar(
            select(Message).where(Message.channel == "whatsapp", Message.whatsapp_message_id == external_id)
        )
        if existing:
            return {"stored": True, "duplicate": True, "message_id": existing.id}
        customer, conversation = self._chat_for(location_id, normalized)
        obj = Message(location_id=location_id, conversation_id=conversation.id, direction=direction,
                      channel="whatsapp", body=self._extract_body(message),
                      whatsapp_message_id=external_id, status=status)
        try:
            obj.created_at = datetime.fromtimestamp(int(message["timestamp"]), tz=timezone.utc)
        except (KeyError, TypeError, ValueError):
            pass
        self.session.add(obj)
        self.session.flush()
        return {"stored": True, "duplicate": False, "message_id": obj.id,
                "conversation_id": conversation.id, "customer_id": customer.id}

    def echo(self, *, location_id: int, message: dict) -> dict:
        """smb_message_echoes: staff replied from the WhatsApp Business app."""
        result = self._store(location_id, message.get("to", ""), message, direction="outbound", status="sent")
        if result.get("stored") and not result.get("duplicate"):
            # A person is talking from the app: pause the AI exactly like an Inbox reply does.
            related = f"customer:{result['customer_id']}"
            open_task = self.session.scalar(select(Task).where(
                Task.location_id == location_id, Task.type == "human_handoff",
                Task.status == "open", Task.related_entity == related))
            if open_task is None:
                self.session.add(Task(location_id=location_id, type="human_handoff", status="open", priority="normal",
                                      related_entity=related,
                                      details={"reason": "Staff replied from the WhatsApp Business app",
                                               "conversation_id": result["conversation_id"]}))
                self.session.flush()
        return result

    def history(self, *, location_id: int, message: dict) -> dict:
        """history: one past message from the app, imported once at signup. Never answered by the AI."""
        thread = str(message.get("_thread", ""))
        inbound = "".join(c for c in str(message.get("from", "")) if c.isdigit()) == "".join(c for c in thread if c.isdigit())
        return self._store(location_id, thread, message,
                           direction="inbound" if inbound else "outbound",
                           status="received" if inbound else "sent")

    def contact(self, *, location_id: int, item: dict) -> dict:
        """smb_app_state_sync: a contact saved in the app. Names the customer; removals are ignored."""
        contact = item.get("contact") or {}
        normalized = "".join(ch for ch in str(contact.get("phone_number", "")) if ch.isdigit())
        name = (contact.get("full_name") or contact.get("first_name") or "").strip()
        if item.get("action") != "add" or not normalized or not name:
            return {"updated": False}
        customer, _ = self._chat_for(location_id, normalized, name=name)
        if (customer.first_name, customer.last_name) == ("New", "Customer"):
            first, _, last = name.partition(" ")
            customer.first_name, customer.last_name = first, last
            self.session.flush()
        return {"updated": True, "customer_id": customer.id}

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
