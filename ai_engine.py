from openai import OpenAI
client = OpenAI()

def classify_message(message):
    prompt = f"""
Classify into ONE:
- booking
- pricing
- repair
- chat

Message: "{message}"
"""

    response = client.chat.completions.create(
        model="gpt-4.1-mini",
        messages=[{"role": "user", "content": prompt}]
    )

    return response.choices[0].message.content.strip().lower()
