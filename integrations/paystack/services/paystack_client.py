"""Thin Paystack HTTP client. Business logic stays in services."""
from __future__ import annotations

import os
import time
from typing import Any, Callable

import requests

BASE_URL = "https://api.paystack.co"


class PaystackAPIError(RuntimeError):
    def __init__(self, message: str, *, status_code: int | None = None, payload: Any = None):
        super().__init__(message)
        self.status_code = status_code
        self.payload = payload


class PaystackClient:
    def __init__(self, secret_key: str | None = None, timeout: float = 20.0, http_request: Callable[..., Any] | None = None):
        self.secret_key = (secret_key or os.getenv("PAYSTACK_SECRET_KEY", "")).strip()
        if not self.secret_key:
            raise ValueError("PAYSTACK_SECRET_KEY is not configured")
        self.timeout = timeout
        self._http_request = http_request or requests.request

    def request(self, method: str, path: str, *, params=None, json=None) -> dict[str, Any]:
        method = method.upper()
        # Safe reads may be retried automatically. POSTs are only retried when
        # the caller supplies a stable Paystack reference (initialize does).
        max_attempts = 3 if method in {"GET", "HEAD"} else 1
        last_error: Exception | None = None
        for attempt in range(max_attempts):
            try:
                response = self._http_request(
                    method,
                    f"{BASE_URL}{path}",
                    headers={"Authorization": f"Bearer {self.secret_key}", "Content-Type": "application/json"},
                    params=params,
                    json=json,
                    timeout=self.timeout,
                )
                try:
                    payload = response.json()
                except ValueError:
                    payload = {"message": response.text}
                if response.status_code < 200 or response.status_code >= 300 or payload.get("status") is False:
                    raise PaystackAPIError(
                        payload.get("message", "Paystack request failed"),
                        status_code=response.status_code,
                        payload=payload,
                    )
                return payload
            except (requests.Timeout, requests.ConnectionError) as exc:
                last_error = exc
                if attempt + 1 == max_attempts:
                    raise PaystackAPIError("Paystack network request failed") from exc
                time.sleep(0.25 * (2**attempt))
        raise PaystackAPIError("Paystack request failed") from last_error

    def initialize_transaction(self, *, email: str, amount_subunits: int, reference: str, callback_url: str | None = None, metadata: dict | None = None, plan: str | None = None):
        body = {"email": email, "amount": amount_subunits, "reference": reference}
        if callback_url:
            body["callback_url"] = callback_url
        if metadata is not None:
            body["metadata"] = metadata
        if plan:
            body["plan"] = plan
        return self.request("POST", "/transaction/initialize", json=body)["data"]

    def verify_transaction(self, reference: str):
        return self.request("GET", f"/transaction/verify/{reference}")["data"]

    def create_customer(self, *, email: str, first_name: str | None = None, last_name: str | None = None, phone: str | None = None, metadata: dict | None = None):
        body = {"email": email}
        for key, value in (("first_name", first_name), ("last_name", last_name), ("phone", phone), ("metadata", metadata)):
            if value is not None:
                body[key] = value
        return self.request("POST", "/customer", json=body)["data"]

    def create_plan(self, *, name: str, amount_subunits: int, interval: str, invoice_limit: int | None = None):
        body = {"name": name, "amount": amount_subunits, "interval": interval}
        if invoice_limit is not None:
            body["invoice_limit"] = invoice_limit
        return self.request("POST", "/plan", json=body)["data"]

    def create_subscription(self, *, customer: str, plan: str):
        return self.request("POST", "/subscription", json={"customer": customer, "plan": plan})["data"]

    def disable_subscription(self, *, code: str, email_token: str):
        return self.request("POST", "/subscription/disable", json={"code": code, "token": email_token})["data"]

    def get_subscription(self, code: str):
        return self.request("GET", f"/subscription/{code}")["data"]

    def refund(self, *, transaction: str, amount_subunits: int | None = None):
        body = {"transaction": transaction}
        if amount_subunits is not None:
            body["amount"] = amount_subunits
        return self.request("POST", "/refund", json=body)["data"]

    def charge_authorization(self, *, email: str, amount_subunits: int, authorization_code: str):
        return self.request(
            "POST",
            "/transaction/charge_authorization",
            json={"email": email, "amount": amount_subunits, "authorization_code": authorization_code},
        )["data"]
