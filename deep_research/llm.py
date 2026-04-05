from __future__ import annotations

import json
import os
import urllib.request
from typing import Any


class OpenAICompatibleLLM:
    def __init__(self, settings: dict[str, Any]) -> None:
        self.settings = settings or {}

    @property
    def enabled(self) -> bool:
        api_key = os.environ.get(self.settings.get("api_key_env", "DEEP_RESEARCH_API_KEY"), "").strip()
        api_url = str(self.settings.get("api_url", "")).strip()
        model = str(self.settings.get("model", "")).strip()
        return bool(self.settings.get("enabled")) and bool(api_key and api_url and model)

    def chat(
        self,
        messages: list[dict[str, str]],
        *,
        temperature: float | None = None,
        max_tokens: int | None = None,
    ) -> str:
        api_key = os.environ.get(self.settings.get("api_key_env", "DEEP_RESEARCH_API_KEY"), "").strip()
        api_url = str(self.settings.get("api_url", "")).strip()
        model = str(self.settings.get("model", "")).strip()
        if not api_key or not api_url or not model:
            return ""

        payload: dict[str, Any] = {
            "model": model,
            "messages": messages,
            "temperature": float(self.settings.get("temperature", 0.2) if temperature is None else temperature),
        }
        if max_tokens:
            payload["max_tokens"] = int(max_tokens)

        req = urllib.request.Request(
            api_url.rstrip("/") + "/chat/completions",
            data=json.dumps(payload).encode("utf-8"),
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {api_key}",
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=90) as resp:
                data = json.loads(resp.read().decode("utf-8"))
            return data["choices"][0]["message"]["content"].strip()
        except Exception:
            return ""
