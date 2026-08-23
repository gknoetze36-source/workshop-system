import hashlib, hmac
from integrations.meta.webhook.signature_verifier import MetaSignatureVerifier


def test_meta_signature_valid_raw_body():
    secret = "s" * 32
    body = b'{"object":"whatsapp_business_account"}'
    sig = "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    assert MetaSignatureVerifier(secret).verify(body, sig)


def test_meta_signature_rejects_changed_body():
    secret = "s" * 32
    body = b'{}'
    sig = "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    assert not MetaSignatureVerifier(secret).verify(b'{ }', sig)


def test_meta_signature_rejects_missing_or_bad_header():
    verifier = MetaSignatureVerifier("s" * 32)
    assert not verifier.verify(b"{}", None)
    assert not verifier.verify(b"{}", "sha1=abc")
