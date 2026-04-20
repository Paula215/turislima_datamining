"""
Scraper estatico para Google Places.

No hace llamadas HTTP. Lee un payload JSON local (respuesta de API)
y devuelve un DataFrame crudo para el normalizador.
"""

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parent.parent
DEFAULT_STATIC_PATH = ROOT / "input" / "google_places_payload.json"


def _extract_items(payload: Any) -> list[dict]:
    """Extrae lista de places desde estructuras comunes de Google Places APIs."""
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]

    if not isinstance(payload, dict):
        return []

    for key in ("results", "places", "candidates", "data", "items"):
        value = payload.get(key)
        if isinstance(value, list):
            return [x for x in value if isinstance(x, dict)]

    if payload.get("place_id") or payload.get("id"):
        return [payload]

    return []


def _get_nested(d: Any, *path: str):
    cur = d
    for p in path:
        if isinstance(cur, dict):
            if p not in cur:
                return None
            cur = cur[p]
            continue

        if isinstance(cur, list):
            try:
                idx = int(p)
            except Exception:
                return None
            if idx < 0 or idx >= len(cur):
                return None
            cur = cur[idx]
            continue

        return None
    return cur


def _coalesce(*values):
    for v in values:
        if v is None:
            continue
        s = str(v).strip() if not isinstance(v, (int, float, bool)) else v
        if s == "" or s == "nan":
            continue
        return v
    return None


def _to_record(item: dict) -> dict:
    display_name = item.get("displayName")
    if isinstance(display_name, dict):
        display_name = display_name.get("text")

    location = item.get("location") or {}
    first_photo_url = _coalesce(
        _get_nested(item, "photos", "0", "url"),
        _get_nested(item, "photos", "0", "name"),
    )

    return {
        "place_id": _coalesce(item.get("place_id"), item.get("id"), item.get("name")),
        "name": _coalesce(item.get("name"), item.get("title"), display_name),
        "title": _coalesce(item.get("title"), item.get("name"), display_name),
        "category": _coalesce(item.get("category"), item.get("categoria"), item.get("primaryType"), item.get("primary_type")),
        "categoria": _coalesce(item.get("categoria"), item.get("category"), item.get("primaryType"), item.get("primary_type")),
        "primary_type": _coalesce(item.get("primary_type"), item.get("primaryType"), item.get("category")),
        "rating": _coalesce(item.get("rating"), item.get("average_rating")),
        "user_ratings_total": _coalesce(item.get("user_ratings_total"), item.get("ratings_total"), item.get("userRatingCount")),
        "reviews_total": _coalesce(item.get("reviews_total"), item.get("user_ratings_total"), item.get("userRatingCount")),
        "reviews": item.get("reviews") if isinstance(item.get("reviews"), list) else None,
        "types": _coalesce(item.get("types"), item.get("categories")),
        "maps_url": _coalesce(item.get("maps_url"), item.get("googleMapsUri"), item.get("url")),
        "url": _coalesce(item.get("url"), item.get("maps_url"), item.get("googleMapsUri")),
        "website": _coalesce(item.get("website"), item.get("websiteUri"), item.get("website_url")),
        "formatted_address": _coalesce(item.get("formatted_address"), item.get("formattedAddress"), item.get("vicinity")),
        "address": _coalesce(item.get("address"), item.get("formatted_address"), item.get("formattedAddress")),
        "distrito": _coalesce(item.get("distrito")),
        "archivo": _coalesce(item.get("archivo")),
        "image_url": _coalesce(item.get("image_url"), item.get("photo_url"), first_photo_url),
        "photo_url": _coalesce(item.get("photo_url"), item.get("image_url")),
        "description": _coalesce(item.get("description"), item.get("shortFormattedAddress")),
        "editorial_summary": _coalesce(item.get("editorial_summary"), _get_nested(item, "editorialSummary", "text")),
        "city": _coalesce(item.get("city"), "Lima"),
        "lat": _coalesce(item.get("lat"), item.get("latitude"), location.get("latitude"), _get_nested(item, "geometry", "location", "lat")),
        "lng": _coalesce(item.get("lng"), item.get("longitude"), location.get("longitude"), _get_nested(item, "geometry", "location", "lng")),
    }


def _load_payload() -> Any:
    inline_payload = os.getenv("GOOGLE_PLACES_STATIC_PAYLOAD", "").strip()
    if inline_payload:
        return json.loads(inline_payload)

    payload_path = Path(os.getenv("GOOGLE_PLACES_STATIC_PATH", str(DEFAULT_STATIC_PATH))).expanduser()
    if not payload_path.exists():
        return []

    with payload_path.open("r", encoding="utf-8") as f:
        return json.load(f)


def run() -> pd.DataFrame:
    print("🔍 Google Places (estatico) iniciado...")
    try:
        payload = _load_payload()
    except Exception as e:
        print(f"  ❌ Error leyendo payload estatico de Google Places: {e}")
        return pd.DataFrame()

    items = _extract_items(payload)
    if not items:
        print("  ⚠️ Sin datos de Google Places (payload ausente o vacio)")
        return pd.DataFrame()

    rows = [_to_record(item) for item in items]
    df = pd.DataFrame(rows)
    df["_source"] = "google_places"
    df["_scraped_at"] = datetime.utcnow().isoformat()

    print(f"✅ Google Places: {len(df)} registros crudos")
    return df


if __name__ == "__main__":
    data = run()
    if not data.empty:
        out = ROOT / "output" / "raw" / "google_places_static_preview.csv"
        out.parent.mkdir(parents=True, exist_ok=True)
        data.to_csv(out, index=False, encoding="utf-8-sig")
        print(f"💾 Preview guardado: {out}")
