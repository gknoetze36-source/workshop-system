from datetime import datetime, timedelta, timezone
from decimal import Decimal

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from models.core import Base, Location, Customer, Vehicle, Conversation, Message, ToolExecution, QuoteLineItem, Approval, Owner
from integrations.ai.conversations.conversation_service import AIConversationService
from integrations.ai.providers.base_provider import AIResponse, ToolCall
from integrations.ai.tools import ServiceAdvisorToolRegistry, ToolContext


def session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    return Session(engine)


class FakeDispatcher:
    def __init__(self):
        self.calls = 0

    def complete(self, request, **kwargs):
        self.calls += 1
        if self.calls == 1:
            return AIResponse(text="", provider="fake", model="fake", tool_calls=[
                ToolCall(id="1", name="capture_customer_context", arguments={"first_name":"Jane","last_name":"Doe","make":"Toyota","model":"Yaris","year":2020,"problem":"service","urgency":"routine"})
            ])
        return AIResponse(text="Thanks Jane. I have your details and can help with the booking.", provider="fake", model="fake")


def test_service_advisor_tool_loop_persists_message_and_tool_execution():
    s = session()
    t = Location(owner=Owner(), name="Workshop")
    s.add(t); s.flush()
    c = Customer(location_id=t.id, first_name="Unknown", last_name="Customer", whatsapp_number="+27111111111")
    s.add(c); s.flush()
    conv = Conversation(location_id=t.id, customer_id=c.id, channel="whatsapp")
    s.add(conv); s.flush()

    service = AIConversationService(FakeDispatcher())
    result = service.reply(session=s, location_id=t.id, conversation_id=conv.id, customer_id=c.id, user_text="I need a service for my Toyota Yaris")
    s.commit()

    assert result["text"].startswith("Thanks Jane")
    assert len(s.scalars(select(Message).where(Message.conversation_id == conv.id)).all()) == 2
    tools = s.scalars(select(ToolExecution).where(ToolExecution.conversation_id == conv.id)).all()
    assert len(tools) == 1
    assert tools[0].tool_name == "capture_customer_context"
    assert s.scalar(select(Vehicle).where(Vehicle.customer_id == c.id)).model == "Yaris"


def test_tool_registry_rejects_cross_location_vehicle():
    s = session()
    a, b = Location(owner=Owner(), name="A"), Location(owner=Owner(), name="B")
    s.add_all([a,b]); s.flush()
    ca=Customer(location_id=a.id,first_name="A",last_name="A",whatsapp_number="+27100000001")
    cb=Customer(location_id=b.id,first_name="B",last_name="B",whatsapp_number="+27100000002")
    s.add_all([ca,cb]); s.flush()
    v=Vehicle(location_id=b.id,customer_id=cb.id,make="Ford",model="Ranger",year=2021)
    s.add(v); s.flush()
    registry=ServiceAdvisorToolRegistry(ToolContext(s,a.id,1,ca.id))
    try:
        registry.execute("get_vehicle", {"vehicle_id":v.id})
        assert False, "cross-location access must fail"
    except ValueError as exc:
        assert "not found" in str(exc) or "belong" in str(exc)


def test_quote_and_repair_authorization_tools_are_removed_from_service_advisor():
    s=session()
    t=Location(owner=Owner(), name="Workshop"); s.add(t); s.flush()
    c=Customer(location_id=t.id,first_name="A",last_name="B",whatsapp_number="+27100000003"); s.add(c); s.flush()
    registry=ServiceAdvisorToolRegistry(ToolContext(s,t.id,1,c.id))
    for tool_name in ("create_quote_draft", "record_approval"):
        try:
            registry.execute(tool_name, {})
            assert False, f"{tool_name} must not be available to PHANTA Service Advisor"
        except ValueError:
            pass
