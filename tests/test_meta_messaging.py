import hashlib
import hmac
import os
import sqlite3
import unittest
from unittest.mock import patch

os.environ.setdefault("SECRET_KEY", "test-secret")
os.environ.setdefault("MESSAGING_TOKEN_ENCRYPTION_KEY", "BW8p_0QsLsxstYCqdNLdtYntSYxEHcWCXwF4beHxRlQ=")

import app
import platform_messaging


class FakeResponse:
    def __init__(self, payload=None):
        self.payload = payload or {"messages": [{"id": "wamid.test"}]}

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


class MetaMessagingTests(unittest.TestCase):
    def test_meta_provider_sends_text_with_bearer_token_and_phone_number_id(self):
        account = {
            "provider": "meta",
            "access_token": platform_messaging.encrypt_access_token("token"),
            "phone_number_id": "12345",
            "workshop_id": "workshop-1",
        }
        with patch("requests.post", return_value=FakeResponse()) as post:
            result = platform_messaging.send_provider_message("+27 82 123 4567", "Hello", account=account)

        self.assertEqual(result["messages"][0]["id"], "wamid.test")
        post.assert_called_once()
        url = post.call_args.args[0]
        kwargs = post.call_args.kwargs
        self.assertEqual(url, "https://graph.facebook.com/v20.0/12345/messages")
        self.assertEqual(kwargs["headers"]["Authorization"], "Bearer token")
        self.assertEqual(kwargs["json"]["messaging_product"], "whatsapp")
        self.assertEqual(kwargs["json"]["to"], "27821234567")
        self.assertEqual(kwargs["json"]["text"]["body"], "Hello")

    def test_provider_resolver_scopes_meta_account_by_workshop_and_phone_number_id(self):
        captured = {}

        def fake_fetch_one(query, args):
            captured["query"] = query
            captured["args"] = args
            return {"provider": "meta", "workshop_id": "workshop-1", "phone_number_id": "phone-1", "is_active": 1}

        with patch.object(platform_messaging, "fetch_one", side_effect=fake_fetch_one):
            account = platform_messaging.active_messaging_account(
                {"workshop_id": "workshop-1"},
                provider="meta",
                phone_number_id="phone-1",
            )

        self.assertEqual(account["provider"], "meta")
        self.assertIn("ma.workshop_id=%s", captured["query"])
        self.assertIn("ma.phone_number_id=%s", captured["query"])
        self.assertEqual(captured["args"], ("whatsapp", "meta", "phone-1", "workshop-1"))

    def test_meta_signature_validation(self):
        body = b'{"entry":[]}'
        secret = "app-secret"
        signature = "sha256=" + hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()

        self.assertTrue(app._validate_meta_signature(body, signature, secret))
        self.assertFalse(app._validate_meta_signature(body, signature, "wrong-secret"))

    def test_meta_phone_number_id_extraction(self):
        payload = {
            "entry": [
                {
                    "changes": [
                        {
                            "value": {
                                "metadata": {
                                    "phone_number_id": "12345",
                                }
                            }
                        }
                    ]
                }
            ]
        }

        self.assertEqual(app._meta_phone_number_id(payload), "12345")

    def test_meta_webhook_routes_by_phone_number_id_and_validates_signature(self):
        payload = b'{"entry":[{"changes":[{"value":{"metadata":{"phone_number_id":"12345"},"messages":[{"from":"27821234567","type":"text","text":{"body":"Hi"}}]}}]}]}'
        signature = "sha256=" + hmac.new(b"app-secret", payload, hashlib.sha256).hexdigest()
        branch = {"id": 7, "franchise_id": 3, "name": "Main"}
        account = {
            "provider": "meta",
            "phone_number_id": "12345",
            "webhook_secret": "app-secret",
            "workshop_id": "workshop-1",
        }

        with patch.object(app, "branch_for_public_booking", return_value=branch), \
             patch.object(app, "fetch_one", return_value={"id": 3, "inbound_webhook_token": "route-token"}), \
             patch.object(app, "active_messaging_account", return_value=account) as active_account, \
             patch.object(app, "_claim_webhook_event", return_value=True), \
             patch.object(app, "_handle_inbound_customer_message") as inbound:
            client = app.app.test_client()
            response = client.post(
                "/webhooks/meta/demo/main/route-token",
                data=payload,
                content_type="application/json",
                headers={"X-Hub-Signature-256": signature},
            )

        self.assertEqual(response.status_code, 200)
        active_account.assert_called_once()
        self.assertEqual(active_account.call_args.kwargs["provider"], "meta")
        self.assertEqual(active_account.call_args.kwargs["phone_number_id"], "12345")
        inbound.assert_called_once_with(branch, "27821234567", "Hi", "WhatsApp", "Received")

    def test_duplicate_meta_webhook_message_is_not_processed_twice(self):
        payload = b'{"entry":[{"changes":[{"value":{"metadata":{"phone_number_id":"12345"},"messages":[{"id":"wamid.1","from":"27821234567","type":"text","text":{"body":"Hi"}}]}}]}]}'
        signature = "sha256=" + hmac.new(b"app-secret", payload, hashlib.sha256).hexdigest()
        branch = {"id": 7, "franchise_id": 3, "name": "Main"}
        account = {
            "provider": "meta",
            "phone_number_id": "12345",
            "webhook_secret": "app-secret",
            "workshop_id": "workshop-1",
        }

        with patch.object(app, "branch_for_public_booking", return_value=branch), \
             patch.object(app, "fetch_one", return_value={"id": 3, "inbound_webhook_token": "route-token"}), \
             patch.object(app, "active_messaging_account", return_value=account), \
             patch.object(app, "_claim_webhook_event", side_effect=[True, False]), \
             patch.object(app, "_handle_inbound_customer_message") as inbound:
            client = app.app.test_client()
            for _ in range(2):
                response = client.post(
                    "/webhooks/meta/demo/main/route-token",
                    data=payload,
                    content_type="application/json",
                    headers={"X-Hub-Signature-256": signature},
                )
                self.assertEqual(response.status_code, 200)

        inbound.assert_called_once_with(branch, "27821234567", "Hi", "WhatsApp", "Received")

    def test_token_encryption_and_decryption(self):
        encrypted = platform_messaging.encrypt_access_token("secret-token")

        self.assertTrue(encrypted.startswith("enc:"))
        self.assertNotIn("secret-token", encrypted)
        self.assertEqual(platform_messaging.decrypt_access_token(encrypted), "secret-token")

    def test_active_meta_phone_and_workshop_uniqueness_constraints(self):
        connection = sqlite3.connect(":memory:")
        connection.execute(
            """
            CREATE TABLE messaging_accounts (
                workshop_id TEXT,
                provider TEXT,
                channel TEXT,
                phone_number_id TEXT,
                is_active INTEGER
            )
            """
        )
        connection.execute(
            "CREATE UNIQUE INDEX idx_messaging_meta_active_workshop ON messaging_accounts(workshop_id, provider) WHERE provider='meta' AND is_active=1"
        )
        connection.execute(
            "CREATE UNIQUE INDEX idx_messaging_meta_active_phone ON messaging_accounts(provider, phone_number_id) WHERE provider='meta' AND is_active=1 AND phone_number_id IS NOT NULL"
        )
        connection.execute("INSERT INTO messaging_accounts VALUES ('workshop-1', 'meta', 'whatsapp', 'phone-1', 1)")

        with self.assertRaises(sqlite3.IntegrityError):
            connection.execute("INSERT INTO messaging_accounts VALUES ('workshop-1', 'meta', 'whatsapp', 'phone-2', 1)")
        with self.assertRaises(sqlite3.IntegrityError):
            connection.execute("INSERT INTO messaging_accounts VALUES ('workshop-2', 'meta', 'whatsapp', 'phone-1', 1)")


if __name__ == "__main__":
    unittest.main()
