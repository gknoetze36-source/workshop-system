from __future__ import annotations

import json
from datetime import datetime, timezone
from sqlalchemy import select

from integrations.ai.providers import AIRequest
from integrations.ai.tools import ServiceAdvisorToolRegistry, ToolContext
from integrations.ai.moderation.output_guard import OutputGuard
from integrations.ai.memory.context_builder import AIContextBuilder
from models.core import Conversation, Message, ConversationSummary


from ai.prompts.system_prompts import SERVICE_ADVISOR_SYSTEM_PROMPT

DEFAULT_SYSTEM_PROMPT = SERVICE_ADVISOR_SYSTEM_PROMPT

# Every turn resends the system prompt, the context blob, the tool definitions and
# the transcript, so the transcript is the only part that grows without bound.
# These two caps keep a long-running WhatsApp thread from turning every reply into
# an ever more expensive request.
MAX_HISTORY_MESSAGES = 20
MAX_HISTORY_CHARS = 6000

# A WhatsApp reply is two or three sentences. Without a ceiling a single confused
# turn can bill for a full-length essay that the output guard then rejects for
# exceeding the customer message length anyway.
MAX_REPLY_TOKENS = 600

# What the customer sees when the model produced something the guard refused.
# Silence is not an acceptable answer to a person waiting on WhatsApp.
GUARD_FALLBACK_TEXT = (
    "Sorry, I can't answer that one myself. I've passed this to the workshop team "
    "and someone will come back to you."
)


class AIConversationService:
    """Single-loop Service Advisor orchestration with allowlisted server-side tools."""

    def __init__(self, dispatcher, output_guard=None, *, prompt_registry=None, context_builder=None, summary_service=None):
        self.dispatcher = dispatcher
        self.output_guard = output_guard or OutputGuard()
        self.prompt_registry = prompt_registry
        self.context_builder = context_builder or AIContextBuilder()
        self.summary_service = summary_service

    def reply(self, *, session, location_id: int, conversation_id: int, customer_id: int,
              user_text: str, send_message=None, deliver_response=None, booking_service=None,
              persist_inbound: bool = True, system: str | None = None, max_tool_rounds: int = 5):
        conversation = session.scalar(select(Conversation).where(
            Conversation.id == conversation_id,
            Conversation.location_id == location_id,
            Conversation.customer_id == customer_id,
        ))
        if not conversation:
            raise ValueError("conversation not found for location/customer")
        if not user_text or not user_text.strip():
            raise ValueError("user_text is required")

        if persist_inbound:
            inbound = Message(
                location_id=location_id, conversation_id=conversation_id, direction="inbound",
                channel=conversation.channel, body=user_text[:4000], status="received",
            )
            session.add(inbound)
            session.flush()

        messages = self._history(session, conversation_id)
        ctx = ToolContext(session=session, location_id=location_id, conversation_id=conversation_id, customer_id=customer_id)
        registry = ServiceAdvisorToolRegistry(
            ctx, send_message=send_message, booking_service=booking_service, current_user_text=user_text
        )
        tool_defs = registry.definitions()
        context = self._context(session, location_id, customer_id, conversation_id)

        base_prompt = system
        if not base_prompt and self.prompt_registry:
            base_prompt = self.prompt_registry.get("service_advisor_system", DEFAULT_SYSTEM_PROMPT)
        base_prompt = base_prompt or DEFAULT_SYSTEM_PROMPT
        full_system = base_prompt + "\n\nCURRENT PHANTA CONTEXT:\n" + json.dumps(context, default=str)

        tool_rounds = 0
        booking_confirmation_recorded = False
        for _ in range(max(1, max_tool_rounds)):
            response = self.dispatcher.complete(
                AIRequest(
                    messages=messages, model="", system=full_system, tools=tool_defs,
                    max_tokens=MAX_REPLY_TOKENS,
                ),
                task_type="conversation", location_id=location_id, conversation_id=conversation_id,
            )
            if not response.tool_calls:
                text = (response.text or "").strip()
                guard = self.output_guard.validate(text, booking_confirmation_recorded=booking_confirmation_recorded)
                if not guard.allowed:
                    # A refused reply used to raise, which the webhook caught and
                    # logged -- leaving the customer with silence and nobody
                    # aware they were waiting. Hand to a human and answer them.
                    text = self._refuse_safely(registry, guard.reasons)
                if deliver_response is not None:
                    delivery = deliver_response(
                        location_id=location_id,
                        conversation_id=conversation_id,
                        customer_id=customer_id,
                        text=text,
                    )
                    return {"text": text, "message_id": getattr(delivery, "id", None),
                            "delivery": delivery, "tool_rounds": tool_rounds}
                outbound = Message(
                    location_id=location_id, conversation_id=conversation_id, direction="outbound",
                    channel=conversation.channel, body=text, status="queued",
                )
                session.add(outbound)
                session.flush()
                return {"text": text, "message_id": outbound.id, "tool_rounds": tool_rounds}

            tool_rounds += 1
            messages.append({
                "role": "assistant", "content": response.text or "",
                "tool_calls": [
                    {"id": c.id, "name": c.name, "arguments": c.arguments}
                    for c in response.tool_calls
                ],
            })
            for call in response.tool_calls:
                try:
                    result = registry.execute(call.name, call.arguments)
                    if call.name == "confirm_booking" and result.get("booking_confirmation_recorded"):
                        booking_confirmation_recorded = True
                except Exception as exc:
                    # Do not expose server internals; the model gets a safe tool error.
                    result = {"error": "tool execution failed", "tool_name": call.name}
                messages.append({
                    "role": "tool", "tool_call_id": call.id, "name": call.name,
                    "content": json.dumps(result, default=str),
                })

        raise RuntimeError("Service Advisor exceeded maximum tool rounds; human handoff required")

    @staticmethod
    def _refuse_safely(registry, reasons) -> str:
        """Replace a guard-refused reply with a handoff the customer can see.

        Returns the fallback text. Escalation is best-effort: if creating the
        handoff task fails we still answer the customer rather than raising,
        because the alternative is a person on WhatsApp getting nothing at all.
        """
        try:
            registry.execute(
                "escalate_to_human",
                {
                    "reason": "Service Advisor output failed the safety guard: "
                              + "; ".join(reasons),
                    "priority": "high",
                },
            )
        except Exception:  # noqa: BLE001 - never let handoff failure silence the reply
            pass
        return GUARD_FALLBACK_TEXT

    def _history(self, session, conversation_id):
        """The most recent turns, oldest-first, within a character budget.

        This previously ordered ascending with limit(50), which selects the
        OLDEST fifty messages: past fifty messages a customer's recent turns
        stopped reaching the model entirely while the request kept growing.
        Order descending to take the newest, then reverse for chronological
        order, and stop early once the budget is spent.
        """
        rows = session.scalars(
            select(Message)
            .where(Message.conversation_id == conversation_id)
            .order_by(Message.created_at.desc())
            .limit(MAX_HISTORY_MESSAGES)
        ).all()

        history: list[dict] = []
        budget = MAX_HISTORY_CHARS
        for m in rows:  # newest first
            body = m.body or ""
            if len(body) > budget:
                break
            budget -= len(body)
            history.append(
                {"role": "user" if m.direction == "inbound" else "assistant", "content": body}
            )
        history.reverse()
        return history

    def _context(self, session, location_id, customer_id, conversation_id):
        from models.core import Customer, Vehicle, Booking, Location, Owner
        location = session.scalar(select(Location).where(Location.id == location_id, Location.active.is_(True)))
        if not location:
            raise ValueError("location not found")
        owner = session.scalar(select(Owner).where(Owner.id == location.owner_id, Owner.active.is_(True)))
        if not owner:
            raise ValueError("owner not found for location")
        c = session.scalar(select(Customer).where(
            Customer.id == customer_id, Customer.location_id == location_id, Customer.deleted_at.is_(None)))
        if not c:
            raise ValueError("customer not found")
        vehicles = session.scalars(select(Vehicle).where(Vehicle.location_id == location_id, Vehicle.customer_id == customer_id)).all()
        bookings = session.scalars(select(Booking).where(
            Booking.location_id == location_id, Booking.customer_id == customer_id,
            Booking.status.not_in(["cancelled", "completed"])
        ).order_by(Booking.start_time.asc()).limit(10)).all()
        summary = session.scalar(select(ConversationSummary).where(
            ConversationSummary.location_id == location_id,
            ConversationSummary.customer_id == customer_id,
            ConversationSummary.conversation_id == conversation_id,
        ).order_by(ConversationSummary.created_at.desc()).limit(1))
        from ai.service_advisor.customer_detection import CustomerDetector
        identity = CustomerDetector(session, location_id).find_or_create(c.whatsapp_number)
        return self.context_builder.build(
            owner={"id": owner.id, "name": owner.name, "email": owner.email},
            location={"id": location.id, "name": location.name, "industry": location.industry},
            customer={"id": c.id, "first_name": c.first_name, "last_name": c.last_name},
            vehicles=[{"id": v.id, "make": v.make, "model": v.model, "year": v.year, "mileage": v.mileage} for v in vehicles],
            bookings=[{"id": b.id, "date": b.start_time.date().isoformat(), "arrival": "morning", "status": b.status, "service_type": b.service_type} for b in bookings],
            summary=summary.summary_text if summary else None,
            identity_kind=identity.kind,
            last_vehicle_id=identity.last_vehicle_id,
        )

    def close_and_summarize(self, *, session, location_id: int, conversation_id: int, summary_text: str):
        conversation = session.scalar(select(Conversation).where(
            Conversation.id == conversation_id, Conversation.location_id == location_id))
        if not conversation:
            raise ValueError("conversation not found")
        conversation.ended_at = datetime.now(timezone.utc)
        summary = ConversationSummary(
            location_id=location_id, customer_id=conversation.customer_id,
            conversation_id=conversation_id, summary_text=summary_text[:4000],
        )
        session.add(summary)
        session.flush()
        return summary
