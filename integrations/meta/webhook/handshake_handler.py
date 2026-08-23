"""Meta webhook GET verification handshake."""
from __future__ import annotations


class MetaHandshakeHandler:
    def __init__(self, verify_token: str):
        if not verify_token:
            raise ValueError("Meta webhook verify token is required")
        self.verify_token = verify_token

    def verify(self, mode: str | None, token: str | None, challenge: str | None) -> str:
        if mode != "subscribe":
            raise ValueError("Invalid Meta webhook mode")
        if not token or not challenge or not self._constant_time_equal(token, self.verify_token):
            raise PermissionError("Invalid Meta webhook verify token")
        return challenge

    @staticmethod
    def _constant_time_equal(a: str, b: str) -> bool:
        import hmac
        return hmac.compare_digest(a.encode(), b.encode())
