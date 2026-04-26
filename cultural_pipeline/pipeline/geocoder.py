"""
geocoder.py
===========
Resuelve lat/lon para eventos sin coordenadas usando Google Geocoding API v4.
Solo opera sobre registros con entity_type=event y lat IS NULL.

Prioridad de dirección a geocodear:
  1. campo `direccion`  (texto estructurado)
  2. campo `lugar` + ", Lima, Perú"
  3. sin dirección → skip, lat/lon queda NULL

Cache: output/geocoding_cache.json  (dirección → {lat, lng})
       Se lee al inicio y se escribe al final del run.
Errores: logs/geocoding_errors.log  (una línea por fallo)

Variables de entorno:
  GOOGLE_GEOCODING_API_KEY  — requerida; el paso se omite si no está
"""

import json
import logging
import os
import time
import urllib.parse
from pathlib import Path
from typing import Optional

import pygeohash
import requests

log = logging.getLogger(__name__)

GEOCODING_ENDPOINT = "https://geocode.googleapis.com/v4/geocode/address/{query}?key={key}"
RATE_LIMIT_SECONDS = 0.1

ROOT = Path(__file__).parent.parent
CACHE_PATH = ROOT / "output" / "geocoding_cache.json"
ERRORS_LOG_PATH = ROOT / "logs" / "geocoding_errors.log"


# ---------------------------------------------------------------------------
# Cache helpers
# ---------------------------------------------------------------------------

def _load_cache() -> dict:
    if CACHE_PATH.exists():
        try:
            with open(CACHE_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def _save_cache(cache: dict) -> None:
    CACHE_PATH.parent.mkdir(exist_ok=True)
    with open(CACHE_PATH, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)


def _log_error(poi_id: str, address: str, reason: str) -> None:
    ERRORS_LOG_PATH.parent.mkdir(exist_ok=True)
    with open(ERRORS_LOG_PATH, "a", encoding="utf-8") as f:
        f.write(f"{poi_id} | {address} | {reason}\n")


# ---------------------------------------------------------------------------
# API call
# ---------------------------------------------------------------------------

def _geocode_one(address: str, api_key: str) -> Optional[tuple[float, float]]:
    """Llama a la API y devuelve (lat, lng) o None si falla."""
    url = GEOCODING_ENDPOINT.format(
        query=urllib.parse.quote(address, safe=""),
        key=api_key,
    )
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        # Intenta estructura v4: results[].geocode.location.latitude
        results = data.get("results") or []
        if not results:
            return None

        first = results[0]

        # v4: results[].location.latitude
        loc = first.get("location") or {}
        if loc.get("latitude") is not None:
            return float(loc["latitude"]), float(loc["longitude"])

        # v4 nested: results[].geocode.location.latitude
        loc2 = (first.get("geocode") or {}).get("location") or {}
        if loc2.get("latitude") is not None:
            return float(loc2["latitude"]), float(loc2["longitude"])

        # v3 fallback: results[].geometry.location.lat
        loc3 = (first.get("geometry") or {}).get("location") or {}
        if loc3.get("lat") is not None:
            return float(loc3["lat"]), float(loc3["lng"])

        return None

    except requests.HTTPError as e:
        raise RuntimeError(f"HTTP {e.response.status_code}") from e
    except Exception as e:
        raise RuntimeError(str(e)) from e


# ---------------------------------------------------------------------------
# Probe — una sola llamada para validar el endpoint antes de bulk
# ---------------------------------------------------------------------------

def probe(address: str = "Gran Biblioteca Pública de Lima, Lima, Perú") -> dict:
    """
    Hace UNA llamada real a la API e imprime la respuesta cruda + el resultado
    extraído. Úsala para verificar que el endpoint y la key funcionan antes
    de lanzar el procesamiento masivo.
    """
    api_key = os.getenv("GOOGLE_GEOCODING_API_KEY")
    if not api_key:
        raise RuntimeError("GOOGLE_GEOCODING_API_KEY no configurada en el entorno")

    url = GEOCODING_ENDPOINT.format(
        query=urllib.parse.quote(address, safe=""),
        key=api_key,
    )
    print(f"🔍 Probe URL: {url[:80]}...{url[-10:]}")

    resp = requests.get(url, timeout=10)
    raw = resp.json()

    print(f"📡 HTTP {resp.status_code}")
    print("📦 Respuesta cruda:")
    print(json.dumps(raw, indent=2, ensure_ascii=False)[:1200])

    result = _geocode_one(address, api_key)
    print(f"\n✅ Resultado extraído: lat={result[0] if result else None}, lng={result[1] if result else None}")
    return raw


# ---------------------------------------------------------------------------
# Resolución de dirección desde un registro
# ---------------------------------------------------------------------------

def _build_address(row) -> Optional[str]:
    import math

    def is_blank(v):
        return v is None or (isinstance(v, float) and math.isnan(v)) or str(v).strip() in ("", "nan")

    direccion = row.get("direccion")
    if not is_blank(direccion):
        return str(direccion).strip()

    lugar = row.get("lugar")
    if not is_blank(lugar):
        return f"{str(lugar).strip()}, Lima, Perú"

    return None


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def geocode_events(df, run_id: str = "") -> "pd.DataFrame":
    """
    Rellena lat, lng y geo_hash para eventos con lat NULL.
    Devuelve el DataFrame modificado in-place (también como return value).

    Si GOOGLE_GEOCODING_API_KEY no está en el entorno, retorna df sin cambios.
    """
    import pandas as pd

    api_key = os.getenv("GOOGLE_GEOCODING_API_KEY")
    if not api_key:
        log.warning("⏭️  Geocoding omitido: GOOGLE_GEOCODING_API_KEY no configurada")
        return df

    import math

    def is_null(v):
        return v is None or (isinstance(v, float) and math.isnan(v))

    mask = (
        df["entity_type"].astype(str).str.lower().eq("event")
        & df["lat"].apply(is_null)
    )
    pending = df[mask]

    if pending.empty:
        log.info("✅ Todos los eventos ya tienen coordenadas, geocoding no necesario")
        return df

    log.info(f"📍 Geocodificando {len(pending)} eventos sin coordenadas...")

    cache = _load_cache()
    hits = misses = errors = skipped = 0

    for idx, row in pending.iterrows():
        address = _build_address(row)
        if address is None:
            skipped += 1
            continue

        if address in cache:
            lat, lng = cache[address]["lat"], cache[address]["lng"]
            hits += 1
        else:
            try:
                result = _geocode_one(address, api_key)
            except RuntimeError as e:
                errors += 1
                _log_error(str(row.get("poi_id", idx)), address, str(e))
                log.debug(f"  ⚠️  {address[:60]} → error: {e}")
                continue

            if result is None:
                errors += 1
                _log_error(str(row.get("poi_id", idx)), address, "no results")
                log.debug(f"  ⚠️  {address[:60]} → sin resultados")
                continue

            lat, lng = result
            cache[address] = {"lat": lat, "lng": lng}
            misses += 1
            time.sleep(RATE_LIMIT_SECONDS)

        df.at[idx, "lat"] = lat
        df.at[idx, "lng"] = lng
        df.at[idx, "geo_hash"] = pygeohash.encode(lat, lng, precision=7)

    _save_cache(cache)

    log.info(
        f"📍 Geocoding completo — resueltos: {hits + misses} "
        f"(caché: {hits}, API: {misses}) | sin dirección: {skipped} | errores: {errors}"
    )
    return df
