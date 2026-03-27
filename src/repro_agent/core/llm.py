from __future__ import annotations

import json
import os
import socket
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any

from .config import LLMConfig


@dataclass
class LLMResponse:
    content: str
    model: str
    provider: str
    raw: dict[str, Any]


class LLMError(RuntimeError):
    pass


class OpenAICompatibleClient:
    def __init__(self, config: LLMConfig):
        self.config = config

    def api_key(self) -> str:
        return os.getenv(self.config.api_key_env, "").strip()

    def is_enabled(self) -> bool:
        return bool(self.config.enabled and self.api_key())

    def complete(
        self,
        messages: list[dict[str, str]],
        *,
        model: str | None = None,
        temperature: float | None = None,
        max_tokens: int | None = None,
        response_format: dict[str, Any] | None = None,
    ) -> LLMResponse:
        if not self.is_enabled():
            raise LLMError(f"LLM provider {self.config.provider} is not configured via {self.config.api_key_env}")

        payload = {
            "model": model or self.config.default_model,
            "messages": messages,
            "temperature": self.config.temperature if temperature is None else temperature,
            "max_tokens": self.config.max_tokens if max_tokens is None else max_tokens,
        }
        if response_format:
            payload["response_format"] = response_format
        req = urllib.request.Request(
            url=self.config.base_url.rstrip("/") + "/chat/completions",
            data=json.dumps(payload).encode("utf-8"),
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.api_key()}",
                "User-Agent": "paper-repro-agent/1.0",
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=self.config.timeout_seconds) as response:
                raw = json.loads(response.read().decode("utf-8"))
        except (TimeoutError, socket.timeout) as exc:  # pragma: no cover - network path
            raise LLMError(f"LLM request timed out after {self.config.timeout_seconds}s") from exc
        except urllib.error.HTTPError as exc:  # pragma: no cover - network path
            body = exc.read().decode("utf-8", errors="ignore")
            raise LLMError(f"LLM HTTP error {exc.code}: {body}") from exc
        except urllib.error.URLError as exc:  # pragma: no cover - network path
            raise LLMError(f"LLM transport error: {exc}") from exc

        choices = raw.get("choices", [])
        if not choices:
            raise LLMError("LLM returned no choices")
        message = choices[0].get("message", {})
        content = str(message.get("content", ""))
        return LLMResponse(
            content=content,
            model=str(raw.get("model", payload["model"])),
            provider=self.config.provider,
            raw=raw,
        )

    def complete_json(
        self,
        messages: list[dict[str, str]],
        *,
        model: str | None = None,
        temperature: float | None = None,
        max_tokens: int | None = None,
    ) -> tuple[dict[str, Any], LLMResponse]:
        try:
            response = self.complete(
                messages,
                model=model,
                temperature=temperature,
                max_tokens=max_tokens,
                response_format={"type": "json_object"},
            )
        except LLMError as exc:
            text = str(exc).lower()
            if "response_format" not in text and "json_object" not in text and "400" not in text:
                raise
            response = self.complete(messages, model=model, temperature=temperature, max_tokens=max_tokens)
        try:
            return _extract_json_object(response.content), response
        except ValueError as exc:
            repaired = self._repair_json_object(
                content=response.content,
                model=model,
                max_tokens=max_tokens,
            )
            if repaired is not None:
                return repaired
            raise LLMError(f"LLM did not return valid JSON: {exc}") from exc

    def _repair_json_object(
        self,
        *,
        content: str,
        model: str | None,
        max_tokens: int | None,
    ) -> tuple[dict[str, Any], LLMResponse] | None:
        text = (content or "").strip()
        if not text:
            return None

        # Keep repair prompts bounded to avoid oversized retries on long model outputs.
        clipped = text[:12000]
        repair_messages = [
            {
                "role": "system",
                "content": (
                    "You repair malformed JSON. Return exactly one valid JSON object, "
                    "no markdown and no extra text."
                ),
            },
            {
                "role": "user",
                "content": (
                    "Repair this into valid JSON object while preserving original keys and values as much as possible. "
                    "If a value is truncated or ambiguous, use null.\n\n"
                    + clipped
                ),
            },
        ]
        try:
            repaired_response = self.complete(
                repair_messages,
                model=model,
                temperature=0.0,
                max_tokens=max_tokens,
                response_format={"type": "json_object"},
            )
        except LLMError:
            return None
        try:
            return _extract_json_object(repaired_response.content), repaired_response
        except ValueError:
            return None


def _extract_json_object(text: str) -> dict[str, Any]:
    text = text.strip()
    if text.startswith("```"):
        parts = [part for part in text.split("```") if part.strip()]
        for part in parts:
            candidate = part.strip()
            if candidate.startswith("json"):
                candidate = candidate[4:].strip()
            try:
                loaded = json.loads(candidate)
                if isinstance(loaded, dict):
                    return loaded
            except json.JSONDecodeError:
                continue

    start = text.find("{")
    end = text.rfind("}")
    if start < 0 or end <= start:
        raise ValueError("no JSON object found")
    candidate = text[start : end + 1]
    try:
        loaded = json.loads(candidate)
        if isinstance(loaded, dict):
            return loaded
    except json.JSONDecodeError:
        pass

    decoder = json.JSONDecoder()
    for index, char in enumerate(text):
        if char != "{":
            continue
        try:
            loaded, _ = decoder.raw_decode(text[index:])
        except json.JSONDecodeError:
            continue
        if isinstance(loaded, dict):
            return loaded
    raise ValueError("root JSON value is not an object")
