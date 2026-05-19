import re


E164_RE = re.compile(r"^\+[1-9][0-9]{7,14}$")


def normalize_phone(value):
    text = str(value or "").strip()
    if text.startswith("whatsapp:"):
        text = text.split(":", 1)[1]
    text = re.sub(r"[\s().-]+", "", text)
    return text


def is_valid_phone(value):
    return bool(E164_RE.fullmatch(normalize_phone(value)))
