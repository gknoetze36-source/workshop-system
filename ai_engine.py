import os
from functools import lru_cache


def classify_message(message):
    text = (message or "").strip().lower()
    if any(word in text for word in ["book", "booking", "appointment", "date available", "slot"]):
        return "booking"
    if any(word in text for word in ["price", "pricing", "cost", "quote", "how much"]):
        return "pricing"
    if any(word in text for word in ["repair", "fix", "noise", "problem", "issue", "brake", "engine"]):
        return "repair"
    if any(word in text for word in ["hours", "open", "close", "location", "address", "where"]):
        return "chat"
    if os.environ.get("ENABLE_AI_INTENT_CLASSIFICATION", "").lower() != "true":
        return "chat"
    return _classify_with_ai(text[:500])


@lru_cache(maxsize=512)
def _classify_with_ai(text):
    if not os.environ.get("OPENAI_API_KEY"):
        return "chat"

    from openai import OpenAI

    client = OpenAI()
    prompt = f"""
Classify into ONE:
- booking
- pricing
- repair
- chat

Message: "{text}"
"""
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
    )
    return response.choices[0].message.content.strip().lower()
