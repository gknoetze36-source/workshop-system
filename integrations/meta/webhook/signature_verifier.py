"""Meta webhook X-Hub-Signature-256 verification."""
from __future__ import annotations

import hashlib
import hmac


class MetaWebhookSignatureError(ValueError):
    pass


class MetaSignatureVerifier:
    """Verify Meta webhook signatures against the untouched raw request body."""

    HEADER_PREFIX = "sha256="

    def __init__(self, app_secret: str):
        if not app_secret:
            raise ValueError("Meta app secret is required for webhook verification")
        self.app_secret = app_secret

    def verify(self, raw_body: bytes, signature_header: str | None) -> bool:
        if not isinstance(raw_body, (bytes, bytearray)):
            raise TypeError("raw_body must be bytes")
        if not signature_header or not signature_header.startswith(self.HEADER_PREFIX):
            return False
        supplied = signature_header[len(self.HEADER_PREFIX):].strip()
        if len(supplied) != 64:
            return False
        try:
            int(supplied, 16)
        except ValueError:
            return False
        expected = hmac.new(self.app_secret.encode("utf-8"), bytes(raw_body), hashlib.sha256).hexdigest()
        return hmac.compare_digest(expected, supplied)

    def require_valid(self, raw_body: bytes, signature_header: str | None) -> None:
        if not self.verify(raw_body, signature_header):
            raise MetaWebhookSignatureError("Invalid Meta webhook signature")
