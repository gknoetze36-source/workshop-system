"""Paystack environment/key validation."""
from __future__ import annotations
import os

class PaystackConfigurationError(RuntimeError):
    pass

def get_paystack_keys() -> tuple[str, str]:
    secret = os.getenv("PAYSTACK_SECRET_KEY", "").strip()
    public = os.getenv("PAYSTACK_PUBLIC_KEY", "").strip()
    if not secret or not public:
        raise PaystackConfigurationError("PAYSTACK_SECRET_KEY and PAYSTACK_PUBLIC_KEY must be configured")
    return secret, public

def is_test_mode(secret_key: str | None = None) -> bool:
    key = secret_key or os.getenv("PAYSTACK_SECRET_KEY", "")
    return key.startswith("sk_test_")
