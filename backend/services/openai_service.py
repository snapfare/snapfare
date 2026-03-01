import os
from typing import Any, Dict

try:
    import openai
except Exception:
    openai = None

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

if openai and OPENAI_API_KEY:
    openai.api_key = OPENAI_API_KEY


def generate_suggestions(preferences: Dict[str, Any]) -> Dict[str, Any]:
    if not openai or not OPENAI_API_KEY:
        return {"status": "disabled", "reason": "OpenAI not configured"}
    try:
        # Placeholder: return echoed preferences
        # Replace with actual OpenAI call as needed
        return {"status": "ok", "suggestions": [
            {"route": "MAD → JFK", "reason": "Buen precio en invierno"},
            {"route": "BCN → HND", "reason": "Ofertas en primavera"},
        ], "input": preferences}
    except Exception as e:
        return {"status": "error", "error": str(e)}
