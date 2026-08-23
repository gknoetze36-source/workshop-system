"""WhatsApp Cloud API outbound messaging client.

Phase 9 intentionally keeps this thin: all Meta calls go through GraphApiClient,
while policy (24-hour window, templates, persistence and retries) lives in the
MessagingService.
"""
from __future__ import annotations

from typing import Any, Mapping

from ..services.graph_api_client import GraphApiClient


class MetaMessageClient:
    def __init__(self, graph: GraphApiClient):
        self.graph = graph

    def send_text(self, *, access_token: str, phone_number_id: str, to: str, body: str) -> dict[str, Any]:
        self._validate_recipient(to)
        if not body or len(body) > 4096:
            raise ValueError("WhatsApp text body must be between 1 and 4096 characters")
        return self.graph.post_with_token(
            access_token,
            f"/{phone_number_id}/messages",
            json_data={
                "messaging_product": "whatsapp",
                "recipient_type": "individual",
                "to": to,
                "type": "text",
                "text": {"preview_url": False, "body": body},
            },
        )

    def send_template(
        self,
        *,
        access_token: str,
        phone_number_id: str,
        to: str,
        name: str,
        language_code: str,
        components: list[Mapping[str, Any]] | None = None,
    ) -> dict[str, Any]:
        self._validate_recipient(to)
        if not name or len(name) > 255:
            raise ValueError("template name is required")
        language_code = language_code.strip()
        if not language_code:
            raise ValueError("template language code is required")
        template: dict[str, Any] = {"name": name, "language": {"code": language_code}}
        if components:
            template["components"] = [dict(c) for c in components]
        return self.graph.post_with_token(
            access_token,
            f"/{phone_number_id}/messages",
            json_data={
                "messaging_product": "whatsapp",
                "recipient_type": "individual",
                "to": to,
                "type": "template",
                "template": template,
            },
        )

    @staticmethod
    def _validate_recipient(to: str) -> None:
        if not to or not to.isdigit() or len(to) < 7 or len(to) > 15:
            raise ValueError("WhatsApp recipient must be an E.164-style digits-only number")
