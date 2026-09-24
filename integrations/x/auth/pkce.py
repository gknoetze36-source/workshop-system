"""PKCE (RFC 7636) for X's OAuth 2.0 Authorization Code flow.

X is the first integration in this codebase that requires PKCE --
Meta and Google's existing OAuth flows use a plain authorization code
exchange with a client secret and need no code_verifier/code_challenge
at all. This is genuinely new, not a duplication of anything Meta's or
Google's auth code already does.

S256 only (code_challenge_method=S256): the "plain" method X's own docs
show in examples is also accepted, but is materially weaker (the
challenge sent in the authorize URL, visible in browser history and
referrer headers, is identical to the verifier presented at token
exchange). RFC 7636 recommends S256 whenever the client can compute
SHA-256, which every Python runtime can.
"""
from __future__ import annotations

import base64
import hashlib
import secrets
from dataclasses import dataclass


@dataclass(frozen=True)
class PkcePair:
    verifier: str
    challenge: str
    method: str = "S256"


def generate_pkce_pair() -> PkcePair:
    """A fresh, single-use verifier/challenge pair. The verifier must be
    stored server-side (XOAuthSession.code_verifier) between building
    the authorize URL and handling the callback -- it is never sent to
    X until the token exchange, and never appears in the authorize URL
    itself (only its SHA-256 hash, the challenge, does)."""
    verifier = _base64url_no_padding(secrets.token_bytes(64))  # RFC 7636: 43-128 chars; this yields ~86
    digest = hashlib.sha256(verifier.encode("ascii")).digest()
    challenge = _base64url_no_padding(digest)
    return PkcePair(verifier=verifier, challenge=challenge)


def _base64url_no_padding(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")
