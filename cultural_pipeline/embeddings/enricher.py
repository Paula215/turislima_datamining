"""enricher.py
============

Optional enrichment step to improve event similarity embeddings.

Goal: produce extra semantic text signals (caption/tags/summary) that can be
appended to `texto_embedding` before generating embeddings.

This module is intentionally provider-agnostic and controlled via env vars.
If not configured, it will no-op.

Environment variables:
- DEEPSEEK_API_KEY: required to enable DeepSeek calls.
- DEEPSEEK_BASE_URL: optional (default: https://api.deepseek.com)
- DEEPSEEK_TEXT_MODEL: optional (default: deepseek-chat)
- DEEPSEEK_VISION_MODEL: optional; if set, tries a vision call for image caption.
- DEEPSEEK_TIMEOUT_SECONDS: optional (default: 30)
- DEEPSEEK_MAX_EVENTS: optional; cap enrichment per run (default: 200)

Notes:
- DeepSeek APIs may differ across accounts/regions. This code assumes an
  OpenAI-compatible `POST /v1/chat/completions` interface when base_url already
  includes scheme/host (the path is appended).
- If the request format is not compatible, set DEEPSEEK_VISION_MODEL empty and
  use text-only enrichment; or adjust base_url/path upstream.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any, Optional

import requests


@dataclass(frozen=True)
class Enrichment:
    image_caption: Optional[str] = None
    tags: Optional[list[str]] = None
    summary: Optional[str] = None
    provider: str = "deepseek"
    provider_model: Optional[str] = None


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def is_deepseek_enabled() -> bool:
    return bool(os.getenv("DEEPSEEK_API_KEY"))


class DeepSeekClient:
    def __init__(self):
        self.api_key = os.getenv("DEEPSEEK_API_KEY")
        self.base_url = (os.getenv("DEEPSEEK_BASE_URL") or "https://api.deepseek.com").rstrip("/")
        self.text_model = os.getenv("DEEPSEEK_TEXT_MODEL") or "deepseek-chat"
        self.vision_model = os.getenv("DEEPSEEK_VISION_MODEL")  # optional
        self.timeout_seconds = _env_float("DEEPSEEK_TIMEOUT_SECONDS", 30)

        if not self.api_key:
            raise RuntimeError("DEEPSEEK_API_KEY no configurada")

    def _post_chat_completions(self, payload: dict[str, Any]) -> dict[str, Any]:
        url = f"{self.base_url}/v1/chat/completions"
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        resp = requests.post(url, headers=headers, data=json.dumps(payload), timeout=self.timeout_seconds)
        resp.raise_for_status()
        return resp.json()

    def _extract_text(self, response: dict[str, Any]) -> str:
        # OpenAI-compatible: choices[0].message.content
        choices = response.get("choices") or []
        if not choices:
            return ""
        msg = (choices[0].get("message") or {})
        content = msg.get("content")
        if isinstance(content, str):
            return content
        # Some APIs return a list of parts; join text parts.
        if isinstance(content, list):
            parts = []
            for p in content:
                if isinstance(p, dict) and p.get("type") in ("text", "output_text") and p.get("text"):
                    parts.append(str(p.get("text")))
            return "\n".join(parts)
        return ""

    def generate_text_tags_and_summary(self, title: str, description: str, event_type: str, place: str) -> Enrichment:
        system = (
            "Eres un asistente que extrae metadatos semánticos para recomendaciones de eventos. "
            "Responde SOLO en JSON válido (sin markdown)."
        )
        user = {
            "title": title,
            "type": event_type,
            "place": place,
            "description": description,
            "task": "Return tags (array of short strings) and a 1-2 sentence Spanish summary.",
            "format": {"tags": ["string"], "summary": "string"},
        }
        payload = {
            "model": self.text_model,
            "temperature": 0.2,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": json.dumps(user, ensure_ascii=False)},
            ],
        }
        raw = self._extract_text(self._post_chat_completions(payload)).strip()
        try:
            parsed = json.loads(raw)
        except Exception:
            # Best-effort fallback: no structured output.
            return Enrichment(tags=None, summary=None, provider_model=self.text_model)

        tags = parsed.get("tags")
        if not isinstance(tags, list):
            tags = None
        else:
            tags = [str(t).strip() for t in tags if str(t).strip()]
            tags = tags[:20] if tags else None
        summary = parsed.get("summary")
        summary = str(summary).strip() if summary else None
        return Enrichment(tags=tags, summary=summary, provider_model=self.text_model)

    def generate_image_caption(self, image_url: str) -> Enrichment:
        if not self.vision_model:
            return Enrichment(image_caption=None, provider_model=None)

        # NOTE: This is an OpenAI-style message format using image_url. If your
        # DeepSeek account uses a different vision schema, disable vision by
        # unsetting DEEPSEEK_VISION_MODEL.
        payload = {
            "model": self.vision_model,
            "temperature": 0.2,
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": "Genera una caption corta en español para esta imagen (máx 15 palabras)."},
                        {"type": "image_url", "image_url": {"url": image_url}},
                    ],
                }
            ],
        }
        caption = self._extract_text(self._post_chat_completions(payload)).strip()
        caption = caption if caption else None
        return Enrichment(image_caption=caption, provider_model=self.vision_model)


def enrich_event(event: dict[str, Any]) -> Enrichment:
    """Best-effort enrichment for a single event.

    Expected keys (all optional): titulo, descripcion, tipo, lugar, imagen_url
    """
    if not is_deepseek_enabled():
        return Enrichment(provider="deepseek", provider_model=None)

    client = DeepSeekClient()

    title = str(event.get("titulo") or "").strip()
    description = str(event.get("descripcion") or "").strip()
    event_type = str(event.get("tipo") or "").strip()
    place = str(event.get("lugar") or "").strip()
    image_url = str(event.get("imagen_url") or "").strip()

    text_enrichment = client.generate_text_tags_and_summary(title, description, event_type, place)
    caption_enrichment = client.generate_image_caption(image_url) if image_url else Enrichment()

    # Merge
    return Enrichment(
        image_caption=caption_enrichment.image_caption,
        tags=text_enrichment.tags,
        summary=text_enrichment.summary,
        provider="deepseek",
        provider_model=text_enrichment.provider_model or caption_enrichment.provider_model,
    )


def append_enrichment_to_texto_embedding(texto_embedding: str, enrichment: Enrichment) -> str:
    parts: list[str] = [texto_embedding or ""]

    if enrichment.image_caption:
        parts.append(f"Imagen: {enrichment.image_caption}")
    if enrichment.summary:
        parts.append(f"Resumen: {enrichment.summary}")
    if enrichment.tags:
        parts.append("AI_Tags: " + ", ".join(enrichment.tags))

    # Keep delimiter consistent with normalizer
    joined = " | ".join(p.strip() for p in parts if p and p.strip())
    return joined


def max_events_to_enrich() -> int:
    return _env_int("DEEPSEEK_MAX_EVENTS", 200)
