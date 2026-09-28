"""DEFECT-001 and DEFECT-002 repairs.

Before this: when the Service Advisor exhausted its tool-call rounds
without producing a final reply, reply() raised RuntimeError uncaught.
routes/webhooks.py's generic except Exception logged it and returned 200
to Meta -- the customer got silence, and despite the exception's own
message saying "human handoff required", no Task was ever created and
no human was ever notified.

Separately, even the one escalation path that did work (_refuse_safely(),
for a guard-refused reply) never stopped the AI from auto-replying to the
same customer's next message -- there was no gate anywhere checking for
an open handoff before calling advisor.reply() again.

Both repairs are additive: the existing, working success and
guard-refusal paths are unchanged (test_service_advisor_guardrails.py and
test_phase12_service_advisor.py, both still passing, cover those).
"""
from datetime import datetime, timezone
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from models.core import Base, Location, Customer, Conversation, Message, Task, Owner
from integrations.ai.conversations.conversation_service import AIConversationService
from integrations.ai.providers.base_provider import AIResponse, ToolCall


def session():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    return Session(engine)


def seed():
    s = session()
    location = Location(owner=Owner(), name="Repair Test Workshop")
    s.add(location)
    s.flush()
    customer = Customer(location_id=location.id, first_name="A", last_name="B", whatsapp_number="27820000099")
    s.add(customer)
    s.flush()
    conversation = Conversation(location_id=location.id, customer_id=customer.id, channel="whatsapp")
    s.add(conversation)
    s.flush()
    return s, location, customer, conversation


class _NeverFinishesDispatcher:
    """Always calls an unrecognized tool, so the loop never produces a
    final text response and max_tool_rounds is genuinely exhausted --
    the exact condition that previously raised RuntimeError uncaught."""

    def __init__(self):
        self.calls = 0

    def complete(self, request, **kwargs):
        self.calls += 1
        return AIResponse(
            text="",
            provider="test",
            model="test",
            tool_calls=[ToolCall(id=f"call-{self.calls}", name="not_a_real_tool", arguments={})],
        )


# --------------------------------------------------------------------------
# DEFECT-001: max-tool-rounds must escalate and reply, not raise silently
# --------------------------------------------------------------------------

def test_exhausting_tool_rounds_no_longer_raises():
    s, location, customer, conversation = seed()
    advisor = AIConversationService(_NeverFinishesDispatcher())

    # Previously: pytest.raises(RuntimeError) here.
    result = advisor.reply(
        session=s, location_id=location.id, conversation_id=conversation.id,
        customer_id=customer.id, user_text="a complex multi-step request",
        max_tool_rounds=3,
    )
    s.commit()

    assert result["text"].strip(), "the customer must receive something, not silence"


def test_exhausting_tool_rounds_creates_a_real_handoff_task():
    s, location, customer, conversation = seed()
    advisor = AIConversationService(_NeverFinishesDispatcher())

    advisor.reply(
        session=s, location_id=location.id, conversation_id=conversation.id,
        customer_id=customer.id, user_text="a complex multi-step request",
        max_tool_rounds=3,
    )
    s.commit()

    tasks = s.scalars(select(Task).where(Task.location_id == location.id)).all()
    assert len(tasks) == 1
    assert tasks[0].type == "human_handoff"
    assert tasks[0].status == "open"
    assert tasks[0].related_entity == f"customer:{customer.id}"


def test_exhausting_tool_rounds_delivers_through_the_normal_channel():
    s, location, customer, conversation = seed()
    advisor = AIConversationService(_NeverFinishesDispatcher())
    delivered = []

    def deliver(**kwargs):
        delivered.append(kwargs)
        return type("Delivery", (), {"id": 777})()

    result = advisor.reply(
        session=s, location_id=location.id, conversation_id=conversation.id,
        customer_id=customer.id, user_text="hi", deliver_response=deliver,
        max_tool_rounds=3,
    )
    s.commit()

    assert len(delivered) == 1, "must go through deliver_response exactly like a normal reply"
    assert result["message_id"] == 777


def test_the_normal_success_path_is_unaffected():
    """The refactor that extracted _deliver_and_record() must not change
    behaviour for a reply that finishes normally."""
    s, location, customer, conversation = seed()

    class _FinishesImmediately:
        def complete(self, request, **kwargs):
            return AIResponse(text="All booked in for Tuesday.", provider="test", model="test")

    result = AIConversationService(_FinishesImmediately()).reply(
        session=s, location_id=location.id, conversation_id=conversation.id,
        customer_id=customer.id, user_text="book me in",
    )
    s.commit()

    assert result["text"] == "All booked in for Tuesday."
    assert len(s.scalars(select(Task).where(Task.location_id == location.id)).all()) == 0, (
        "a clean reply must not create any handoff task"
    )


# --------------------------------------------------------------------------
# DEFECT-002: an open handoff must pause the AI for that customer
# --------------------------------------------------------------------------

def test_open_handoff_task_is_discoverable_by_the_exact_key_the_webhook_checks():
    """routes/webhooks.py's new gate queries
    Task.related_entity == f"customer:{customer_id}" with type="human_handoff"
    and status="open" -- exactly what escalate_to_human() (via
    _refuse_safely(), reached by both the guard-refusal and the now-fixed
    max-rounds path) already writes. This is the contract test for that
    gate without re-driving the whole webhook route."""
    s, location, customer, conversation = seed()
    task = Task(
        location_id=location.id, type="human_handoff",
        related_entity=f"customer:{customer.id}", status="open", priority="high",
        details={"reason": "test"},
    )
    s.add(task)
    s.flush()

    found = s.scalar(
        select(Task).where(
            Task.location_id == location.id,
            Task.type == "human_handoff",
            Task.status == "open",
            Task.related_entity == f"customer:{customer.id}",
        )
    )
    assert found is not None
    assert found.id == task.id


def test_a_resolved_handoff_task_is_not_matched():
    s, location, customer, conversation = seed()
    task = Task(
        location_id=location.id, type="human_handoff",
        related_entity=f"customer:{customer.id}", status="resolved", priority="high",
        details={},
    )
    s.add(task)
    s.flush()

    found = s.scalar(
        select(Task).where(
            Task.location_id == location.id,
            Task.type == "human_handoff",
            Task.status == "open",
            Task.related_entity == f"customer:{customer.id}",
        )
    )
    assert found is None, "a resolved handoff must not keep gating future replies forever"
