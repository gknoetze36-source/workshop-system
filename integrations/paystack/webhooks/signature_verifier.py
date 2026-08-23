import hashlib, hmac

def verify_signature(raw_body: bytes, signature: str | None, secret_key: str) -> bool:
    if not signature: return False
    expected = hmac.new(secret_key.encode(), raw_body, hashlib.sha512).hexdigest()
    supplied = signature.removeprefix("sha512=")
    return hmac.compare_digest(expected, supplied)
