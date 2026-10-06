"""WhatsApp inbox: staff see chats, replying pauses the AI, handing back resumes
it, and one workshop can never read or answer another workshop's chats."""
import re
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import select

from ai.service_advisor.runtime import AI_LABEL, label_ai_text
from database import SessionLocal, execute_db, query_db, utc_now
from integrations.meta.messaging import messaging_service
from models.core import Conversation, Customer, Message, Task
from tests.unit.test_settings_permission_boundaries import client_with_roles  # noqa: F401  (fixture)


def _seed_chat(location_id, *, last_inbound=None):
    db = SessionLocal()
    cust = Customer(location_id=location_id, first_name="Naledi", last_name="Inbox",
                    whatsapp_number="+2782" + uuid.uuid4().hex[:7])
    db.add(cust); db.flush()
    conv = Conversation(location_id=location_id, customer_id=cust.id, channel="whatsapp")
    db.add(conv); db.flush()
    at = last_inbound or datetime.now(timezone.utc)
    db.add(Message(location_id=location_id, conversation_id=conv.id, direction="inbound",
                   channel="whatsapp", body="Hi, is my Hilux ready?", created_at=at))
    db.add(Message(location_id=location_id, conversation_id=conv.id, direction="outbound",
                   channel="whatsapp", body=AI_LABEL + "Let me check that for you.", status="sent",
                   created_at=at + timedelta(seconds=5)))
    db.commit()
    ids = conv.id, cust.id
    db.close()
    return ids


def _token(client):
    html = client.get("/dashboard/inbox").get_data(as_text=True)
    return re.search(r'name="csrf-token" content="([^"]+)"', html).group(1)


def _handoffs(location_id, customer_id):
    db = SessionLocal()
    try:
        return [t.status for t in db.scalars(select(Task).where(
            Task.location_id == location_id, Task.type == "human_handoff",
            Task.related_entity == f"customer:{customer_id}"))]
    finally:
        db.close()


@pytest.fixture
def fake_send(monkeypatch):
    sent = []

    def send_auto(self, *, location_id, conversation_id, to, body, **_):
        from integrations.meta.messaging.session_window import WhatsAppSessionWindow
        if not WhatsAppSessionWindow.is_open(self.session, location_id=location_id, conversation_id=conversation_id):
            raise messaging_service.MetaSessionWindowClosedError("customer_service_window_closed")
        msg = Message(location_id=location_id, conversation_id=conversation_id, direction="outbound",
                      channel="whatsapp", body=body, status="sent")
        self.session.add(msg); self.session.flush()
        sent.append((to, body))
        return msg

    monkeypatch.setattr(messaging_service.MetaMessagingService, "send_auto", send_auto)
    return sent


def test_label_ai_text_marks_ai_once():
    assert label_ai_text("Hello") == AI_LABEL + "Hello"
    assert label_ai_text(AI_LABEL + "Hello") == AI_LABEL + "Hello"


def test_inbox_lists_chat_and_marks_ai_replies(client_with_roles):
    ctx = client_with_roles
    _seed_chat(ctx["location_id"])
    ctx["login_as"](ctx["reception_email"])
    html = ctx["client"].get("/dashboard/inbox").get_data(as_text=True)
    assert "Naledi Inbox" in html and "Hi, is my Hilux ready?" in html
    assert "AI assistant &middot;" in html


def test_staff_reply_sends_and_pauses_ai_then_hand_back_resumes(client_with_roles, fake_send):
    ctx = client_with_roles
    conv_id, cust_id = _seed_chat(ctx["location_id"])
    ctx["login_as"](ctx["owner_email"])
    token = _token(ctx["client"])
    ctx["client"].post(f"/dashboard/inbox/{conv_id}/reply", data={"body": "Yes, ready at 4pm.", "csrf_token": token})
    assert fake_send and fake_send[0][1] == "Yes, ready at 4pm."  # staff text is NOT AI-labelled
    assert _handoffs(ctx["location_id"], cust_id) == ["open"]

    ctx["client"].post(f"/dashboard/inbox/{conv_id}/reply", data={"body": "See you then.", "csrf_token": token})
    assert _handoffs(ctx["location_id"], cust_id) == ["open"]  # no duplicate pause

    ctx["client"].post(f"/dashboard/inbox/{conv_id}/resume-ai", data={"csrf_token": token})
    assert _handoffs(ctx["location_id"], cust_id) == ["resolved"]


def test_reply_after_24_hours_is_refused_without_pausing_ai(client_with_roles, fake_send):
    ctx = client_with_roles
    conv_id, cust_id = _seed_chat(ctx["location_id"], last_inbound=datetime.now(timezone.utc) - timedelta(days=2))
    ctx["login_as"](ctx["owner_email"])
    token = _token(ctx["client"])
    resp = ctx["client"].post(f"/dashboard/inbox/{conv_id}/reply",
                              data={"body": "Hello?", "csrf_token": token}, follow_redirects=True)
    assert "more than 24 hours" in resp.get_data(as_text=True)
    assert fake_send == [] and _handoffs(ctx["location_id"], cust_id) == []


def test_other_workshops_chat_is_invisible_and_unanswerable(client_with_roles, fake_send):
    ctx = client_with_roles
    suffix = uuid.uuid4().hex[:10]
    execute_db("INSERT INTO owners (name, email, active, created_at, updated_at) VALUES (%s,%s,TRUE,%s,%s)",
               ("Other", f"other-{suffix}@test.example", utc_now(), utc_now()))
    owner_id = query_db("SELECT id FROM owners WHERE email=%s", (f"other-{suffix}@test.example",), one=True)["id"]
    execute_db("INSERT INTO locations (owner_id, name, industry, active, created_at, updated_at) "
               "VALUES (%s,%s,'workshop',TRUE,%s,%s)", (owner_id, f"Other {suffix}", utc_now(), utc_now()))
    other_loc = query_db("SELECT id FROM locations WHERE owner_id=%s", (owner_id,), one=True)["id"]
    other_conv, other_cust = _seed_chat(other_loc)

    ctx["login_as"](ctx["owner_email"])
    html = ctx["client"].get(f"/dashboard/inbox?c={other_conv}").get_data(as_text=True)
    assert "Hi, is my Hilux ready?" not in html
    token = _token(ctx["client"])
    ctx["client"].post(f"/dashboard/inbox/{other_conv}/reply", data={"body": "x", "csrf_token": token})
    assert fake_send == [] and _handoffs(other_loc, other_cust) == []
