import hashlib, hmac
from integrations.paystack.webhooks.signature_verifier import verify_signature

def test_signature_uses_raw_body_and_sha512():
    secret = "sk_test_secret"
    body = b'{"event":"charge.success","data":{"reference":"abc"}}'
    digest = hmac.new(secret.encode(), body, hashlib.sha512).hexdigest()
    assert verify_signature(body, digest, secret)
    assert not verify_signature(body + b" ", digest, secret)
