"""Test OpenRouter API key validity."""
import os
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv("wqsa.env", override=True)

key = os.getenv("OPENROUTER_API_KEY", "")
print(f"Key loaded : {key[:20]}...{key[-6:]}")
print(f"Key length : {len(key)}")

client = OpenAI(base_url="https://openrouter.ai/api/v1", api_key=key)

try:
    resp = client.chat.completions.create(
        model="openai/gpt-4o-mini",
        messages=[{"role": "user", "content": "Say hello in one word."}],
        max_tokens=10,
    )
    print(f"\nSUCCESS: {resp.choices[0].message.content}")
except Exception as e:
    print(f"\nFAILED: {e}")
