"""PHANTA outbound WhatsApp policy, persistence and retry orchestration."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Mapping

from sqlalchemy import select
from sqlalchemy.orm import Session

from models.core import Conversation, Message
from models.integration_models import MetaBusinessConnection, MetaMessageAttempt
from ..auth.token_store import MetaTokenStore
from ..services.graph_api_client import GraphApiClient, MetaGraphAPIError
from .message_client import MetaMessageClient
from .retry_policy import MetaRetryPolicy
from .session_window import WhatsAppSessionWindow
from .template_repository import MetaTemplateRepository


class MetaMessagingError(RuntimeError):
    pass


class MetaMessagingService:
    def __init__(self, session: Session, *, graph: GraphApiClient, token_store: MetaTokenStore):
        self.session = session
        self.graph = graph
        self.token_store = token_store
        self.client = MetaMessageClient(graph)
        self.retry_policy = MetaRetryPolicy()
        self.templates = MetaTemplateRepository()

    def send_text(
        self, *, location_id: int, conversation_id: int, to: str, body: str,
        now: datetime | None = None, max_attempts: int = 3,
    ) -> Message:
        if not WhatsAppSessionWindow.is_open(self.session, location_id=location_id, conversation_id=conversation_id, now=now):
            raise MetaMessagingError("customer_service_window_closed: use an approved WhatsApp template")
        return self._send(
            location_id=location_id, conversation_id=conversation_id, to=to, body=body,
            sender=lambda token, phone: self.client.send_text(
                access_token=token, phone_number_id=phone, to=to, body=body
            ),
            max_attempts=max_attempts,
        )

    def send_utility_template(
        self, *, location_id: int, conversation_id: int, to: str, name: str,
        language_code: str = "en_ZA", components: list[Mapping[str, Any]] | None = None,
        max_attempts: int = 3,
    ) -> Message:
        template = self.templates.get(self.session, location_id=location_id, name=name, language=language_code)
        if template is None:
            raise MetaMessagingError("template_not_registered")
        if template.category.upper() != "UTILITY":
            raise MetaMessagingError("template_category_must_be_utility")
        if template.status.upper() != "APPROVED":
            raise MetaMessagingError(f"template_not_sendable:{template.status}")
        return self._send(
            location_id=location_id, conversation_id=conversation_id, to=to,
            body=f"[template:{name}]",
            sender=lambda token, phone: self.client.send_template(
                access_token=token, phone_number_id=phone, to=to, name=name,
                language_code=language_code, components=components
            ),
            max_attempts=max_attempts,
        )

    def send_auto(
        self, *, location_id: int, conversation_id: int, to: str, body: str,
        template_name: str | None = None, template_language: str = "en_ZA",
        template_components: list[Mapping[str, Any]] | None = None,
    ) -> Message:
        if WhatsAppSessionWindow.is_open(self.session, location_id=location_id, conversation_id=conversation_id):
            return self.send_text(location_id=location_id, conversation_id=conversation_id, to=to, body=body)
        if not template_name:
            raise MetaMessagingError("customer_service_window_closed: approved_template_required")
        return self.send_utility_template(
            location_id=location_id, conversation_id=conversation_id, to=to,
            name=template_name, language_code=template_language, components=template_components
        )

    def _send(self, *, location_id: int, conversation_id: int, to: str, body: str, sender, max_attempts: int) -> Message:
        connection = self.session.scalar(
            select(MetaBusinessConnection).where(
                MetaBusinessConnection.location_id == location_id,
                MetaBusinessConnection.connection_status == "connected",
            )
        )
        if connection is None or not connection.phone_number_id or not connection.encrypted_access_token:
            raise MetaMessagingError("meta_connection_not_ready")

        conversation = self.session.get(Conversation, conversation_id)
        if conversation is None or conversation.location_id != location_id:
            raise MetaMessagingError("conversation_not_found")

        message = Message(
            location_id=location_id, conversation_id=conversation_id, direction="outbound",
            channel="whatsapp", body=body, status="queued"
        )
        self.session.add(message)
        self.session.flush()

        token = self.token_store.get_customer_token(connection)
        attempts = max(1, min(int(max_attempts), self.retry_policy.MAX_ATTEMPTS))

        for attempt_no in range(1, attempts + 1):
            try:
                payload = sender(token, connection.phone_number_id)
                wamid = self._extract_wamid(payload)
                message.whatsapp_message_id = wamid
                message.status = "sent"
                self._record_attempt(message, attempt_no, status="sent", response=payload, retryable=False)
                self.session.flush()
                return message
            except MetaGraphAPIError as exc:
                error = exc.error or {}
                meta_code = str(error.get("code")) if error.get("code") is not None else None
                decision = self.retry_policy.decide(
                    attempt_number=attempt_no, http_status=exc.status_code, meta_error_code=meta_code
                )
                self._record_attempt(
                    message, attempt_no, status="failed", http_status=exc.status_code,
                    meta_error_code=meta_code, error_message=str(exc), response=error or None,
                    retryable=decision.retryable,
                )
                message.status = "failed"
                if not decision.retryable or attempt_no >= attempts:
                    self.session.flush()
                    raise MetaMessagingError(f"meta_send_failed:{meta_code or exc.status_code or 'unknown'}") from exc
                # The service records the retry decision; actual sleeping is avoided
                # inside a web request. A worker can re-run the queued operation.
                message.status = "queued_retry"
                self.session.flush()
                raise MetaMessagingError(f"meta_send_retryable:{decision.delay_seconds}") from exc
        raise MetaMessagingError("meta_send_failed")

    def _record_attempt(self, message: Message, attempt_no: int, *, status: str, http_status: int | None = None,
                        meta_error_code: str | None = None, error_message: str | None = None,
                        response: dict | None = None, retryable: bool = False) -> None:
        self.session.add(MetaMessageAttempt(
            location_id=message.location_id, message_id=message.id, attempt_number=attempt_no,
            status=status, http_status=http_status, meta_error_code=meta_error_code,
            error_message=error_message, response_json=response, retryable=retryable
        ))

    @staticmethod
    def _extract_wamid(payload: dict[str, Any]) -> str:
        messages = payload.get("messages") or []
        if not messages or not messages[0].get("id"):
            raise MetaMessagingError("meta_send_missing_wamid")
        return str(messages[0]["id"])
