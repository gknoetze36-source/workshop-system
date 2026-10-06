"""WhatsApp inbox: staff read and answer customer chats from the dashboard.

A Cloud API number no longer works in the WhatsApp app, so this page is where
a workshop's people see and reply to their customers. It reuses what exists:
messages already live in conversations/messages, replies go through
MetaMessagingService, and "the AI is paused" is the open human_handoff task
the WhatsApp webhook already checks before letting the AI answer.
"""
from __future__ import annotations

from flask import Blueprint, flash, redirect, render_template, request, url_for
from sqlalchemy import func, select

from ai.service_advisor.runtime import AI_LABEL
from database import get_session
from helpers.location import current_location_id
from helpers.permission import OPERATIONAL_ROLES, require_role
from models.core import Conversation, Customer, Message, Task
from services.auth_service import login_required

inbox_bp = Blueprint("inbox", __name__, url_prefix="/dashboard/inbox")

HANDOFF = "human_handoff"


def _open_handoffs(session, location_id: int, customer_id: int) -> list[Task]:
    return list(session.scalars(select(Task).where(
        Task.location_id == location_id, Task.type == HANDOFF, Task.status == "open",
        Task.related_entity == f"customer:{customer_id}",
    )))


def _conversation(session, location_id: int, conversation_id: int) -> Conversation | None:
    conv = session.get(Conversation, conversation_id)
    return conv if conv is not None and conv.location_id == location_id else None


@inbox_bp.get("")
@login_required
@require_role(*OPERATIONAL_ROLES)
def inbox():
    location_id = current_location_id()
    session = get_session()
    try:
        last_at = (
            select(Message.conversation_id, func.max(Message.created_at).label("last_at"))
            .where(Message.location_id == location_id)
            .group_by(Message.conversation_id)
            .subquery()
        )
        rows = session.execute(
            select(Conversation, Customer, last_at.c.last_at)
            .join(last_at, last_at.c.conversation_id == Conversation.id)
            .join(Customer, Customer.id == Conversation.customer_id)
            .where(Conversation.location_id == location_id, Conversation.channel == "whatsapp")
            .order_by(last_at.c.last_at.desc())
            .limit(100)  # ponytail: newest 100 chats, add paging when a workshop outgrows it
        ).all()
        paused = {
            t.related_entity for t in session.scalars(select(Task).where(
                Task.location_id == location_id, Task.type == HANDOFF, Task.status == "open"))
        }
        chats = [{
            "id": conv.id,
            "name": f"{cust.first_name} {cust.last_name}".strip() or cust.whatsapp_number,
            "number": cust.whatsapp_number,
            "last_at": last,
            "paused": f"customer:{cust.id}" in paused,
        } for conv, cust, last in rows]

        selected = request.args.get("c", type=int) or (chats[0]["id"] if chats else None)
        current, messages = None, []
        conv = _conversation(session, location_id, selected) if selected else None
        if conv is not None:
            current = next((c for c in chats if c["id"] == conv.id), None)
            messages = [{
                "inbound": m.direction == "inbound",
                "ai": m.direction == "outbound" and m.body.startswith(AI_LABEL.strip()),
                "body": m.body,
                "at": m.created_at,
                "status": m.status,
            } for m in session.scalars(
                select(Message).where(Message.location_id == location_id, Message.conversation_id == conv.id)
                .order_by(Message.created_at.asc(), Message.id.asc())
            )]
        from integrations.meta.messaging.session_window import WhatsAppSessionWindow
        window_open = bool(conv) and WhatsAppSessionWindow.is_open(
            session, location_id=location_id, conversation_id=conv.id)
        return render_template("inbox.html", chats=chats, current=current,
                               messages=messages, window_open=window_open)
    finally:
        session.close()


@inbox_bp.post("/<int:conversation_id>/reply")
@login_required
@require_role(*OPERATIONAL_ROLES)
def reply(conversation_id: int):
    location_id = current_location_id()
    body = (request.form.get("body") or "").strip()
    back = redirect(url_for("inbox.inbox", c=conversation_id))
    if not body:
        return back
    session = get_session()
    try:
        conv = _conversation(session, location_id, conversation_id)
        if conv is None:
            flash("That chat was not found.", "error")
            return redirect(url_for("inbox.inbox"))
        customer = session.get(Customer, conv.customer_id)
        from integrations.meta.auth.capability_config import WhatsAppMetaConfig
        from integrations.meta.auth.token_store import MetaTokenStore
        from integrations.meta.messaging.messaging_service import (
            MetaMessagingError, MetaMessagingService, MetaSessionWindowClosedError,
        )
        from integrations.meta.services.graph_api_client import GraphApiClient
        service = MetaMessagingService(session, graph=GraphApiClient(WhatsAppMetaConfig.from_env()),
                                       token_store=MetaTokenStore())
        try:
            service.send_auto(location_id=location_id, conversation_id=conv.id,
                              to=customer.whatsapp_number, body=body)
        except MetaSessionWindowClosedError:
            session.rollback()
            flash("This customer last wrote more than 24 hours ago, so WhatsApp only allows "
                  "an approved template message. Ask them to message you first.", "error")
            return back
        except MetaMessagingError as exc:
            session.commit()  # keep the failed message + attempt for the record
            flash(f"WhatsApp did not accept the message ({exc}).", "error")
            return back
        # A person is now talking: keep the AI out of this chat until handed back.
        if not _open_handoffs(session, location_id, customer.id):
            session.add(Task(location_id=location_id, type=HANDOFF, status="open", priority="normal",
                             related_entity=f"customer:{customer.id}",
                             details={"reason": "Staff replied from the inbox", "conversation_id": conv.id}))
        session.commit()
        return back
    finally:
        session.close()


@inbox_bp.post("/<int:conversation_id>/resume-ai")
@login_required
@require_role(*OPERATIONAL_ROLES)
def resume_ai(conversation_id: int):
    location_id = current_location_id()
    session = get_session()
    try:
        conv = _conversation(session, location_id, conversation_id)
        if conv is not None:
            for task in _open_handoffs(session, location_id, conv.customer_id):
                task.status = "resolved"
            session.commit()
            flash("The AI assistant will answer this customer again.", "success")
        return redirect(url_for("inbox.inbox", c=conversation_id))
    finally:
        session.close()
