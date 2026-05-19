import hashlib
import hmac
import os

import requests


PAYSTACK_BASE_URL = "https://api.paystack.co"


def _secret_key():
    key = os.environ.get("PAYSTACK_SECRET_KEY")
    if not key:
        raise RuntimeError("PAYSTACK_SECRET_KEY is required")
    return key


def _headers():
    return {
        "Authorization": f"Bearer {_secret_key()}",
        "Content-Type": "application/json",
    }


def initialize_transaction(email, amount, reference, callback_url="", metadata=None):
    payload = {
        "email": email,
        "amount": int(round(float(amount or 0) * 100)),
        "currency": "ZAR",
        "reference": reference,
        "metadata": metadata or {},
    }
    if callback_url:
        payload["callback_url"] = callback_url
    response = requests.post(f"{PAYSTACK_BASE_URL}/transaction/initialize", json=payload, headers=_headers(), timeout=15)
    response.raise_for_status()
    return response.json()


def verify_transaction(reference):
    response = requests.get(f"{PAYSTACK_BASE_URL}/transaction/verify/{reference}", headers=_headers(), timeout=15)
    response.raise_for_status()
    return response.json()


def valid_webhook_signature(raw_body, signature):
    secret = os.environ.get("PAYSTACK_WEBHOOK_SECRET") or os.environ.get("PAYSTACK_SECRET_KEY")
    if not secret or not signature:
        return False
    expected = hmac.new(secret.encode("utf-8"), raw_body, hashlib.sha512).hexdigest()
    return hmac.compare_digest(expected, signature)
