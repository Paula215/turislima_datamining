"""
Scraper Joinnus (flujo publico)

Flujo implementado:
1) Consulta API /activity/v1/home/search con payload por categoria para inferir paginas.
2) Recorre HTML de descubrir por categoria/pagina y extrae links de eventos.
3) Scrapea cada detalle publico para poblar campos base del evento.

Salida compatible con pipeline/normalizer.py::normalize_joinnus.
"""

from __future__ import annotations

import json
import logging
import math
import os
import re
import time
import argparse
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import quote, urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None


ROOT = Path(__file__).resolve().parent.parent
if load_dotenv is not None:
    load_dotenv(dotenv_path=ROOT / ".env")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


BASE_URL = "https://www.joinnus.com"
DISCOVER_URL = f"{BASE_URL}/descubrir"
API_SEARCH_URL = "https://oapi.joinnus.com/activity/v1/home/search"

# Categorias solicitadas por el usuario.
JOINNUS_CATEGORIES = [
    "theater",
    "art-culture",
    "cine",
    "concerts",
    "food-drinks",
    "cursos-talleres",
    "community-service",
    "sports",
    "entertainment",
    "festivales",
    "stand-up",
    "futbol",
    "trip-adventure",
    "seminarios-conferencias",
]

DISTRICTS_LIMA = [
    "Ate",
    "Barranco",
    "Brena",
    "Carabayllo",
    "Chaclacayo",
    "Chorrillos",
    "Cieneguilla",
    "Comas",
    "El Agustino",
    "Independencia",
    "Jesus Maria",
    "La Molina",
    "La Victoria",
    "Lima",
    "Lince",
    "Los Olivos",
    "Lurin",
    "Magdalena",
    "Miraflores",
    "Pachacamac",
    "Pucusana",
    "Pueblo Libre",
    "Puente Piedra",
    "Punta Hermosa",
    "Punta Negra",
    "Rimac",
    "San Bartolo",
    "San Borja",
    "San Isidro",
    "San Juan de Lurigancho",
    "San Juan de Miraflores",
    "San Luis",
    "San Martin de Porres",
    "San Miguel",
    "Santa Anita",
    "Santa Maria del Mar",
    "Santa Rosa",
    "Santiago de Surco",
    "Surquillo",
    "Villa El Salvador",
    "Villa Maria del Triunfo",
]


def _build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "es-PE,es;q=0.9,en;q=0.8",
            "Content-Type": "application/json",
            "Origin": BASE_URL,
            "Referer": f"{DISCOVER_URL}",
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
        }
    )

    token = (os.getenv("JOINNUS_API_BEARER_TOKEN") or "").strip()
    if token:
        session.headers["Authorization"] = f"Bearer {token}"

    api_key = (os.getenv("JOINNUS_API_KEY") or "").strip()
    if api_key:
        session.headers["x-api-key"] = api_key

    return session


def _safe_json(response: requests.Response) -> Dict[str, Any]:
    try:
        parsed = response.json()
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def _collect_numeric_by_keys(obj: Any, wanted_keys: Set[str]) -> List[int]:
    found: List[int] = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            lk = str(k).lower().replace("-", "_")
            if lk in wanted_keys and isinstance(v, (int, float)):
                found.append(int(v))
            found.extend(_collect_numeric_by_keys(v, wanted_keys))
    elif isinstance(obj, list):
        for item in obj:
            found.extend(_collect_numeric_by_keys(item, wanted_keys))
    return found


def _extract_items(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    for key in ("data", "items", "results", "activities", "events", "list", "hits"):
        value = payload.get(key)
        if isinstance(value, list):
            return [x for x in value if isinstance(x, dict)]
        if isinstance(value, dict):
            for nested_key in ("data", "items", "results", "activities", "events", "list", "hits"):
                nested = value.get(nested_key)
                if isinstance(nested, list):
                    out: List[Dict[str, Any]] = []
                    for item in nested:
                        if not isinstance(item, dict):
                            continue
                        source = item.get("_source")
                        if isinstance(source, dict):
                            out.append(source)
                        else:
                            out.append(item)
                    return out
    return []


def _infer_pages_from_response(payload: Dict[str, Any], page_size: int) -> Optional[int]:
    page_keys = ["pages", "total_pages", "totalpages", "page_count", "pagecount", "last_page", "lastpage"]
    total_keys = ["total", "total_items", "totalitems", "total_count", "totalcount", "count"]

    containers: List[Dict[str, Any]] = []
    if isinstance(payload, dict):
        containers.append(payload)
        for key in ("data", "meta", "pagination"):
            value = payload.get(key)
            if isinstance(value, dict):
                containers.append(value)

    def _get_first_numeric(container: Dict[str, Any], aliases: List[str]) -> Optional[int]:
        normalized = {str(k).lower().replace("-", "_"): v for k, v in container.items()}
        for alias in aliases:
            value = normalized.get(alias)
            if isinstance(value, (int, float)) and value > 0:
                return int(value)
        return None

    page_values: List[int] = []
    for container in containers:
        page_value = _get_first_numeric(container, page_keys)
        if page_value is not None:
            page_values.append(page_value)
    if page_values:
        return max(page_values)

    totals: List[int] = []
    for container in containers:
        total_value = _get_first_numeric(container, total_keys)
        if total_value is not None:
            totals.append(total_value)

    if totals and page_size > 0:
        return int(math.ceil(max(totals) / float(page_size)))

    return None


def _search_api_page(
    session: requests.Session,
    category: str,
    page: int,
    size: int,
) -> Tuple[int, Dict[str, Any]]:
    payload = {
        "categories": [category],
        "order": "latest",
        "page": page,
        "size": size,
    }
    try:
        resp = session.post(API_SEARCH_URL, json=payload, timeout=25)
    except Exception as exc:
        logger.warning("Joinnus API error category=%s page=%s: %s", category, page, exc)
        return 0, {}
    return resp.status_code, _safe_json(resp)


def _discover_total_pages(
    session: requests.Session,
    category: str,
    page_size: int,
    max_pages_hard: int,
) -> Optional[int]:
    status, payload = _search_api_page(session, category, page=1, size=page_size)
    if status != 200:
        logger.warning(
            "API /home/search sin acceso para categoria=%s (status=%s). "
            "Usando fallback HTML progresivo por paginas.",
            category,
            status,
        )
        return None

    inferred = _infer_pages_from_response(payload, page_size)
    if inferred is not None:
        return max(1, min(inferred, max_pages_hard))

    # Si la API no reporta pages explicitamente, avanzamos hasta vacio.
    first_items = _extract_items(payload)
    if not first_items:
        return 1

    total_pages = 1
    for page in range(2, max_pages_hard + 1):
        status_n, payload_n = _search_api_page(session, category, page=page, size=page_size)
        if status_n != 200:
            break
        items_n = _extract_items(payload_n)
        if not items_n:
            break
        total_pages = page
        if len(items_n) < page_size:
            break
    return max(1, total_pages)


def _build_discover_candidates(category: str, page: int) -> List[str]:
    safe = quote(category, safe="-")
    return [
        f"{DISCOVER_URL}/{safe}?page={page}",
        f"{DISCOVER_URL}?category={safe}&page={page}",
        f"{DISCOVER_URL}?categories={safe}&page={page}",
        f"{DISCOVER_URL}?page={page}&category={safe}",
        f"{DISCOVER_URL}?page={page}&categories={safe}",
    ]


def _filter_links_for_category(links: Set[str], category: str) -> Set[str]:
    filtered = {u for u in links if f"/events/{category}/" in u}
    return filtered or links


def _normalize_category_slug(value: Optional[str], fallback: str) -> str:
    raw = str(value or fallback).strip().lower()
    raw = re.sub(r"\s+", "-", raw)
    raw = re.sub(r"[^a-z0-9\-]", "", raw)
    return raw or fallback


def _build_event_url_from_api_item(item: Dict[str, Any], fallback_category: str) -> Optional[str]:
    direct_candidates = [
        item.get("canonicalUrl"),
        item.get("canonical_url"),
        item.get("url"),
        item.get("urlRedirection"),
    ]

    for candidate in direct_candidates:
        if not candidate:
            continue
        raw = str(candidate).strip()
        if not raw:
            continue

        if raw.startswith("http://") or raw.startswith("https://"):
            normalized = _normalize_event_url(raw)
            if normalized:
                return normalized

        if "/events/" in raw:
            normalized = _normalize_event_url(urljoin(BASE_URL, raw))
            if normalized:
                return normalized

    slug = item.get("activityUrl") or item.get("slug") or item.get("activity_slug")
    if not slug:
        return None

    slug = str(slug).strip().strip("/")
    if not slug:
        return None

    # La categoria del item puede venir con label humano; para estabilidad,
    # usamos la categoria consultada en la API.
    category_slug = _normalize_category_slug(fallback_category, fallback_category)
    return _normalize_event_url(f"{BASE_URL}/events/{category_slug}/{slug}")


def _seed_from_api_item(item: Dict[str, Any], fallback_category: str) -> Dict[str, Any]:
    location = item.get("location")
    location_name: Optional[str] = None
    location_address: Optional[str] = None
    if isinstance(location, dict):
        location_name = _clean_text(
            location.get("name")
            or location.get("localName")
            or location.get("venue")
        )
        location_address = _clean_text(
            location.get("address")
            or location.get("addressRef")
            or location.get("streetAddress")
        )
    elif isinstance(location, str):
        location_name = _clean_text(location)

    images = item.get("images")
    image_value: Optional[str] = None
    if isinstance(images, list) and images:
        first = images[0]
        if isinstance(first, dict):
            image_value = _clean_text(
                first.get("url")
                or first.get("image")
                or first.get("imageUrl")
            )
        elif isinstance(first, str):
            image_value = _clean_text(first)
    elif isinstance(images, str):
        image_value = _clean_text(images)

    return {
        "titulo": _clean_text(item.get("title") or item.get("name")),
        "categoria_principal": _clean_text(item.get("activityCategory") or fallback_category),
        "descripcion_corta": _clean_text(item.get("description") or item.get("summary")),
        "fecha_inicio": item.get("date") or item.get("dateStart"),
        "fecha_fin": item.get("dateEnd"),
        "ubicacion_nombre": location_name,
        "ubicacion_direccion": location_address,
        "precio_desde": _to_float(item.get("price")),
        "moneda": _clean_text(item.get("currency")) or "PEN",
        "imagen_principal": image_value,
    }


def _collect_links_api_pages(
    session: requests.Session,
    category: str,
    total_pages: int,
    page_size: int,
) -> Tuple[List[Tuple[int, int, int, Set[str]]], Dict[str, Dict[str, Any]]]:
    results: List[Tuple[int, int, int, Set[str]]] = []
    cumulative_links: Set[str] = set()
    seed_by_url: Dict[str, Dict[str, Any]] = {}

    for page in range(1, total_pages + 1):
        status, payload = _search_api_page(
            session=session,
            category=category,
            page=page,
            size=page_size,
        )
        if status != 200:
            break

        items = _extract_items(payload)
        page_links: Set[str] = set()
        for item in items:
            url = _build_event_url_from_api_item(item, fallback_category=category)
            if url:
                page_links.add(url)
                if url not in seed_by_url:
                    seed_by_url[url] = _seed_from_api_item(item, fallback_category=category)

        page_links = _filter_links_for_category(page_links, category)
        new_links = page_links - cumulative_links
        cumulative_links.update(page_links)

        results.append((page, len(page_links), len(new_links), new_links))

        if not page_links:
            break

    return results, seed_by_url


def _normalize_event_url(href: str) -> Optional[str]:
    if not href:
        return None
    url = urljoin(BASE_URL, href)
    if "/events/" not in url:
        return None
    url = url.split("#", 1)[0].split("?", 1)[0].rstrip("/")
    return url if url.startswith(BASE_URL) else None


def _extract_event_links_from_html(html: str) -> Set[str]:
    soup = BeautifulSoup(html, "lxml")
    links: Set[str] = set()

    selectors = [
        "a.absolute.inset-0.z-10.cursor-pointer[href]",
        "a[aria-label='Ver detalle del evento'][href]",
        "a[href*='/events/'][href]",
    ]
    for selector in selectors:
        for anchor in soup.select(selector):
            url = _normalize_event_url(anchor.get("href", ""))
            if url:
                links.add(url)
    return links


def _collect_links_html(
    driver: webdriver.Chrome,
    category: str,
    page: int,
) -> Set[str]:
    page_links: Set[str] = set()
    candidates = _build_discover_candidates(category, page)

    for url in candidates:
        try:
            driver.get(url)
            time.sleep(2.0)
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight * 0.65);")
            time.sleep(1.0)
            html = driver.page_source
            found = _extract_event_links_from_html(html)
        except Exception as exc:
            logger.debug("No se pudo leer HTML %s: %s", url, exc)
            continue

        if not found:
            continue

        page_links = _filter_links_for_category(found, category)
        break

    return page_links


def _collect_links_html_progressive(
    driver: webdriver.Chrome,
    category: str,
    max_batches: int,
) -> List[Tuple[int, int, int, Set[str]]]:
    """
    Joinnus Discover carga mas eventos por scroll (infinite feed).
    Modelamos cada carga incremental como una "pagina".

    Retorna una lista de tuplas:
    (batch_number, total_links_detectados, nuevos_en_batch, nuevos_links_set)
    """
    results: List[Tuple[int, int, int, Set[str]]] = []
    if max_batches <= 0:
        return results

    safe_category = quote(category, safe="-")
    base_candidates = [
        f"{DISCOVER_URL}/{safe_category}",
        f"{DISCOVER_URL}?category={safe_category}",
        f"{DISCOVER_URL}?categories={safe_category}",
    ]

    loaded = False
    for url in base_candidates:
        try:
            driver.get(url)
            time.sleep(2.5)
            loaded = True
            break
        except Exception:
            continue

    if not loaded:
        return results

    cumulative_links: Set[str] = set()
    stale_streak = 0

    for batch in range(1, max_batches + 1):
        html = driver.page_source
        found = _extract_event_links_from_html(html)
        found = _filter_links_for_category(found, category)

        new_links = found - cumulative_links
        cumulative_links.update(found)
        results.append((batch, len(found), len(new_links), new_links))

        if len(new_links) == 0:
            stale_streak += 1
        else:
            stale_streak = 0

        if batch >= max_batches or stale_streak >= 2:
            break

        # Scroll en dos pasos para activar lazy loading de cards adicionales.
        try:
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(1.6)
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(1.6)
        except Exception:
            break

    return results


def _parse_ld_json(soup: BeautifulSoup) -> Dict[str, Any]:
    for tag in soup.find_all("script", type="application/ld+json"):
        raw = (tag.string or "").strip()
        if not raw:
            continue
        try:
            payload = json.loads(raw)
            if isinstance(payload, list):
                for item in payload:
                    if isinstance(item, dict) and item.get("@type") in {
                        "Event",
                        "MusicEvent",
                        "SocialEvent",
                    }:
                        return item
            elif isinstance(payload, dict) and payload.get("@type") in {
                "Event",
                "MusicEvent",
                "SocialEvent",
            }:
                return payload
        except Exception:
            continue
    return {}


def _meta_content(soup: BeautifulSoup, prop: str) -> Optional[str]:
    tag = soup.find("meta", property=prop)
    if not tag:
        return None
    value = (tag.get("content") or "").strip()
    return value or None


def _clean_text(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    text = re.sub(r"\s+", " ", str(value)).strip()
    return text or None


def _to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    match = re.search(r"(\d+(?:[\.,]\d+)?)", str(value))
    if not match:
        return None
    try:
        return float(match.group(1).replace(",", "."))
    except Exception:
        return None


def _extract_price_from_offers(offers: Any) -> Optional[float]:
    if isinstance(offers, dict):
        return _to_float(offers.get("price"))
    if isinstance(offers, list):
        prices = [_to_float(item.get("price")) for item in offers if isinstance(item, dict)]
        prices = [p for p in prices if p is not None]
        if prices:
            return min(prices)
    return None


def _extract_activity_json(page_source: str) -> Dict[str, Any]:
    patterns = [
        r'"activity"\s*:\s*(\{.*?\})\s*,\s*"blogs"',
        r'"activity"\s*:\s*(\{.*?\})\s*,\s*"related"',
        r'"activity"\s*:\s*(\{.*?\})\s*,\s*"recommended"',
    ]
    for pattern in patterns:
        match = re.search(pattern, page_source, re.DOTALL)
        if not match:
            continue
        try:
            parsed = json.loads(match.group(1))
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            continue
    return {}


def _extract_district(text: Optional[str]) -> Optional[str]:
    if not text:
        return None
    source = str(text).lower()
    for district in DISTRICTS_LIMA:
        if district.lower() in source:
            return district
    return None


def _as_time_from_iso(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    match = re.search(r"T(\d{2}):(\d{2})", str(value))
    if not match:
        return None
    return f"{match.group(1)}:{match.group(2)}"


def _fallback_description(event: Dict[str, Any]) -> str:
    title = _clean_text(event.get("titulo")) or "Evento en Joinnus"
    category = _clean_text(event.get("categoria_principal")) or "cultural"
    date_start = _clean_text(event.get("fecha_inicio")) or "fecha por confirmar"
    venue = _clean_text(event.get("ubicacion_nombre")) or "Lima"
    district = _clean_text(event.get("distrito"))
    if district and district.lower() not in venue.lower():
        venue = f"{venue} ({district})"

    price_from = event.get("precio_desde")
    currency = _clean_text(event.get("moneda")) or "PEN"
    if isinstance(price_from, (int, float)):
        price_text = f"desde {price_from:.2f} {currency}"
    else:
        price_text = "precio por confirmar"

    return (
        f"{title}. Categoria: {category}. "
        f"Inicio: {date_start}. Lugar: {venue}. {price_text}."
    )


def _scrape_event_public(
    url: str,
    session: requests.Session,
    seed: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    seed = seed or {}
    out: Dict[str, Any] = {
        "url": url,
        "titulo": seed.get("titulo"),
        "categoria_principal": seed.get("categoria_principal"),
        "descripcion_completa": None,
        "descripcion_corta": seed.get("descripcion_corta"),
        "fecha_inicio": seed.get("fecha_inicio"),
        "fecha_fin": seed.get("fecha_fin"),
        "ubicacion_nombre": seed.get("ubicacion_nombre"),
        "ubicacion_direccion": seed.get("ubicacion_direccion"),
        "precio_desde": seed.get("precio_desde"),
        "moneda": seed.get("moneda") or "PEN",
        "imagen_principal": seed.get("imagen_principal"),
        "ciudad": "Lima",
        "distrito": None,
    }

    try:
        response = session.get(
            url,
            timeout=25,
            headers={
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "es-PE,es;q=0.9,en;q=0.8",
                "User-Agent": (
                    "Mozilla/5.0 (X11; Linux x86_64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
            },
        )
        if response.status_code >= 400:
            return out

        soup = BeautifulSoup(response.text, "lxml")

        ld = _parse_ld_json(soup)
        if ld:
            out["titulo"] = _clean_text(ld.get("name")) or out["titulo"]
            out["fecha_inicio"] = ld.get("startDate") or out["fecha_inicio"]
            out["fecha_fin"] = ld.get("endDate") or out["fecha_fin"]

            image = ld.get("image")
            if isinstance(image, list):
                out["imagen_principal"] = image[0] if image else out["imagen_principal"]
            elif isinstance(image, str):
                out["imagen_principal"] = image

            location = ld.get("location")
            if isinstance(location, dict):
                out["ubicacion_nombre"] = _clean_text(location.get("name")) or out["ubicacion_nombre"]
                address = location.get("address")
                if isinstance(address, dict):
                    out["ubicacion_direccion"] = (
                        _clean_text(address.get("streetAddress"))
                        or _clean_text(address.get("addressLocality"))
                        or out["ubicacion_direccion"]
                    )
                elif isinstance(address, str):
                    out["ubicacion_direccion"] = _clean_text(address)

            offers = ld.get("offers")
            out["precio_desde"] = _extract_price_from_offers(offers) or out["precio_desde"]
            if isinstance(offers, dict):
                out["moneda"] = offers.get("priceCurrency") or out["moneda"]

        out["titulo"] = out["titulo"] or _meta_content(soup, "og:title")
        out["descripcion_corta"] = out["descripcion_corta"] or _meta_content(soup, "og:description")
        out["imagen_principal"] = out["imagen_principal"] or _meta_content(soup, "og:image")

        path_cat = re.search(r"/events/([^/]+)/", url)
        if path_cat:
            out["categoria_principal"] = path_cat.group(1)

        activity = _extract_activity_json(response.text)
        if activity:
            out["categoria_principal"] = activity.get("category") or out["categoria_principal"]
            out["descripcion_completa"] = _clean_text(activity.get("description")) or out["descripcion_completa"]
            out["descripcion_corta"] = out["descripcion_corta"] or _clean_text(activity.get("description"))
            out["ubicacion_nombre"] = _clean_text(activity.get("localName")) or out["ubicacion_nombre"]
            out["ubicacion_direccion"] = (
                _clean_text(activity.get("address"))
                or _clean_text(activity.get("addressRef"))
                or out["ubicacion_direccion"]
            )
            out["precio_desde"] = _to_float(activity.get("priceFrom")) or out["precio_desde"]
            out["imagen_principal"] = (
                activity.get("imagePosterUrl")
                or activity.get("imageUrl")
                or out["imagen_principal"]
            )

        if not out["descripcion_completa"]:
            desc_block = soup.select_one(".description-html, .content_description")
            if desc_block:
                out["descripcion_completa"] = _clean_text(desc_block.get_text(" ", strip=True))

        if not out["descripcion_completa"]:
            out["descripcion_completa"] = out["descripcion_corta"]

        if out["precio_desde"] is None:
            possible_prices = []
            for node in soup.select("[data-cy*='price'], .price, .ticket-price")[:20]:
                possible_prices.append(_to_float(node.get_text(" ", strip=True)))
            possible_prices = [p for p in possible_prices if p is not None]
            if possible_prices:
                out["precio_desde"] = min(possible_prices)

        district = _extract_district(out["ubicacion_direccion"]) or _extract_district(out["ubicacion_nombre"])
        out["distrito"] = district

    except Exception as exc:
        logger.debug("No se pudo scrapear detalle publico %s: %s", url, exc)

    return out


def _event_record_to_raw(event: Dict[str, Any]) -> Dict[str, Any]:
    date = event.get("fecha_inicio")
    venue = event.get("ubicacion_nombre") or event.get("distrito") or "Lima"
    description = event.get("descripcion_completa") or event.get("descripcion_corta")
    if not _clean_text(description):
        description = _fallback_description(event)

    return {
        "url": event.get("url"),
        "titulo": event.get("titulo"),
        "categoria_principal": event.get("categoria_principal"),
        "descripcion_completa": event.get("descripcion_completa"),
        "descripcion_corta": event.get("descripcion_corta"),
        "fecha_inicio": event.get("fecha_inicio"),
        "fecha_fin": event.get("fecha_fin"),
        "ubicacion_nombre": event.get("ubicacion_nombre"),
        "ubicacion_direccion": event.get("ubicacion_direccion"),
        "precio_desde": event.get("precio_desde"),
        "moneda": event.get("moneda") or "PEN",
        "imagen_principal": event.get("imagen_principal"),
        "ciudad": event.get("ciudad") or "Lima",
        "distrito": event.get("distrito"),
        "source_url": event.get("url"),
        "canonical_url": event.get("url"),
        "title": event.get("titulo"),
        "description": description,
        "category": event.get("categoria_principal"),
        "date": date,
        "time": _as_time_from_iso(date),
        "location": f"Peru, {venue}, Lima",
        "image_url": event.get("imagen_principal"),
        "performer_list": None,
        "ticket_availability": "available",
    }


def _build_driver(headless: bool) -> webdriver.Chrome:
    options = Options()
    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    # Necesario para inspeccionar requests de red y extraer headers runtime.
    options.set_capability("goog:loggingPrefs", {"performance": "ALL"})
    return webdriver.Chrome(options=options)


def _extract_candidate_auth_headers(raw_headers: Dict[str, Any]) -> Dict[str, str]:
    normalized = {}
    for key, value in (raw_headers or {}).items():
        if isinstance(value, str):
            normalized[str(key).lower()] = value

    selected: Dict[str, str] = {}
    for key in (
        "authorization",
        "x-api-key",
        "x-xsrf-token",
        "x-csrf-token",
        "cookie",
        "origin",
        "referer",
        "user-agent",
        "accept-language",
        "content-type",
    ):
        value = normalized.get(key)
        if value:
            selected[key] = value

    has_auth = any(k in selected for k in ("authorization", "x-api-key", "cookie"))
    return selected if has_auth else {}


def _capture_runtime_auth_headers(driver: webdriver.Chrome, timeout_seconds: int = 16) -> Dict[str, str]:
    """
    Intenta capturar headers runtime del frontend (authorization/x-api-key/cookies)
    para reutilizarlos en llamadas API server-side.
    """
    pending: Dict[str, Dict[str, Any]] = {}
    deadline = time.time() + timeout_seconds
    injected_probe = False

    while time.time() < deadline:
        elapsed = timeout_seconds - max(0, int(deadline - time.time()))
        if (not injected_probe) and elapsed >= 4:
            # Forzamos un request al endpoint de busqueda para que aparezca en logs,
            # incluso si la UI no lo dispara de inmediato.
            try:
                driver.execute_async_script(
                    """
                    const done = arguments[0];
                    fetch('https://oapi.joinnus.com/activity/v1/home/search?page=1&size=12', {
                      method: 'POST',
                      headers: {'content-type': 'application/json'},
                      body: JSON.stringify({categories:['theater'], order:'latest', page:1, size:12})
                    }).finally(() => done(true));
                    """
                )
            except Exception:
                pass
            injected_probe = True

        try:
            logs = driver.get_log("performance")
        except Exception:
            logs = []

        for entry in logs:
            try:
                message = json.loads(entry.get("message", "{}")).get("message", {})
            except Exception:
                continue

            method = message.get("method")
            params = message.get("params", {})
            request_id = params.get("requestId", "")

            if method == "Network.requestWillBeSent":
                request = params.get("request", {})
                url = str(request.get("url", ""))
                if "oapi.joinnus.com/activity/v1/home" not in url:
                    continue

                pending[request_id] = {
                    "url": url,
                    "headers": dict(request.get("headers") or {}),
                }
                candidate = _extract_candidate_auth_headers(pending[request_id]["headers"])
                if candidate:
                    return candidate

            elif method == "Network.requestWillBeSentExtraInfo":
                if request_id not in pending:
                    continue
                extra_headers = params.get("headers", {})
                pending[request_id]["headers"].update(extra_headers)
                candidate = _extract_candidate_auth_headers(pending[request_id]["headers"])
                if candidate:
                    return candidate

        time.sleep(0.4)

    # Fallback: evaluar lo que quedo capturado aunque no haya ExtraInfo.
    for request_data in pending.values():
        candidate = _extract_candidate_auth_headers(request_data.get("headers", {}))
        if candidate:
            return candidate
    return {}


def _bootstrap_session_auth_from_browser(session: requests.Session, driver: webdriver.Chrome) -> bool:
    """
    Ejecuta un bootstrap al iniciar la corrida para intentar obtener credenciales
    efimeras del frontend en tiempo real.
    """
    try:
        driver.get(DISCOVER_URL)
        time.sleep(3)
    except Exception as exc:
        logger.debug("No se pudo abrir Discover para bootstrap auth: %s", exc)
        return False

    # Transferimos cookies web a requests para mantener contexto de sesion.
    try:
        for cookie in driver.get_cookies():
            name = cookie.get("name")
            value = cookie.get("value")
            if name and value:
                session.cookies.set(name, value)
    except Exception:
        pass

    headers = _capture_runtime_auth_headers(driver)
    if not headers:
        return False

    for key, value in headers.items():
        if key == "cookie":
            # Requests ya administra cookies; solo usamos header Cookie como fallback.
            session.headers.setdefault("Cookie", value)
            continue
        session.headers[key] = value
    return True


def _resolve_categories() -> List[str]:
    env_categories = (os.getenv("JOINNUS_CATEGORIES") or "").strip()
    if not env_categories:
        return JOINNUS_CATEGORIES

    parsed = [c.strip() for c in env_categories.split(",") if c.strip()]
    if not parsed:
        return JOINNUS_CATEGORIES

    ordered: List[str] = []
    seen: Set[str] = set()
    for cat in parsed:
        if cat not in seen:
            ordered.append(cat)
            seen.add(cat)
    return ordered


def _resolve_max_links(max_links_override: Optional[int]) -> Optional[int]:
    """
    Resuelve el tope de links global de la corrida.

    - `None`: sin limite (extraer todo)
    - `0` o negativo: sin limite
    - `>0`: tope explicito
    """
    if max_links_override is None:
        return None

    value = int(max_links_override)

    if value <= 0:
        return None
    return value


def run(
    max_pages: int = 0,
    events_per_page: int = 12,
    headless: bool = True,
    max_links: Optional[int] = None,
) -> pd.DataFrame:
    """
    Interfaz estandar del pipeline.

    Args:
        max_pages: limite opcional por categoria. Si es 0, usa paginas detectadas por API.
        events_per_page: size del payload API.
        headless: modo headless de Selenium para extraccion HTML.
    """
    categories = _resolve_categories()
    if not categories:
        return pd.DataFrame()

    page_size = max(1, int(events_per_page))
    pages_hard_cap = max(1, int(os.getenv("JOINNUS_MAX_PAGES_HARD", "30")))
    max_links_limit = _resolve_max_links(max_links)

    logger.info("Joinnus scraper iniciado: %s categorias", len(categories))
    session = _build_session()
    driver: Optional[webdriver.Chrome] = None

    try:
        driver = _build_driver(headless=headless)
        has_runtime_auth = _bootstrap_session_auth_from_browser(session=session, driver=driver)
        if has_runtime_auth:
            logger.info("Se capturaron credenciales runtime desde el navegador para la API Joinnus")
        else:
            logger.info("No se capturaron credenciales runtime; se mantendra fallback HTML")

        all_links: Set[str] = set()
        api_seed_by_url: Dict[str, Dict[str, Any]] = {}

        if max_links_limit is None:
            logger.info("Limite de links: sin limite")
        else:
            logger.info("Limite de links: %s", max_links_limit)

        for category in categories:
            pages_for_category = _discover_total_pages(
                session=session,
                category=category,
                page_size=page_size,
                max_pages_hard=pages_hard_cap,
            )
            if pages_for_category is not None:
                target_pages = pages_for_category
                if max_pages > 0:
                    target_pages = min(target_pages, max_pages)
                logger.info("Categoria=%s -> pages=%s (API links)", category, target_pages)

                page_results = _collect_links_api_pages(
                    session=session,
                    category=category,
                    total_pages=target_pages,
                    page_size=page_size,
                )
                if isinstance(page_results, tuple):
                    page_results, seed_chunk = page_results
                    api_seed_by_url.update(seed_chunk)
            else:
                target_pages = max_pages if max_pages > 0 else pages_hard_cap
                logger.info("Categoria=%s -> batches=%s (HTML fallback)", category, target_pages)
                page_results = _collect_links_html_progressive(
                    driver=driver,
                    category=category,
                    max_batches=target_pages,
                )

            if not page_results and pages_for_category is not None:
                # API respondio 200 pero no se pudieron mapear links; fallback HTML.
                target_pages = max_pages if max_pages > 0 else pages_hard_cap
                logger.warning(
                    "Categoria=%s sin links via API mapeada. Activando fallback HTML (%s batches)",
                    category,
                    target_pages,
                )
                page_results = _collect_links_html_progressive(
                    driver=driver,
                    category=category,
                    max_batches=target_pages,
                )

            for batch, total_links, new_count, new_links in page_results:
                logger.info(
                    "  %s page=%s -> %s links (%s nuevos)",
                    category,
                    batch,
                    total_links,
                    new_count,
                )
                all_links.update(new_links)

                if max_links_limit is not None and len(all_links) >= max_links_limit:
                    logger.info("Limite maximo de links alcanzado (%s)", max_links_limit)
                    break

            if max_links_limit is not None and len(all_links) >= max_links_limit:
                break

        if not all_links:
            logger.warning("No se encontraron links de eventos en HTML de Joinnus")
            return pd.DataFrame()

        rows: List[Dict[str, Any]] = []
        for index, event_url in enumerate(sorted(all_links), start=1):
            if max_links_limit is not None and index > max_links_limit:
                break
            event = _scrape_event_public(
                event_url,
                session=session,
                seed=api_seed_by_url.get(event_url),
            )
            rows.append(_event_record_to_raw(event))
            time.sleep(0.15)

        df = pd.DataFrame(rows)
        if df.empty:
            return df

        df["_source"] = "joinnus"
        df["_scraped_at"] = datetime.utcnow().isoformat()
        return df

    except Exception as exc:
        logger.error("Joinnus scraper fallo: %s", exc)
        return pd.DataFrame()
    finally:
        if driver is not None:
            try:
                driver.quit()
            except Exception:
                pass


def main() -> None:
    parser = argparse.ArgumentParser(description="Scraper Joinnus")
    parser.add_argument("--max-pages", type=int, default=0, help="Limite de paginas por categoria (0=automatico)")
    parser.add_argument("--events-per-page", type=int, default=12, help="Tamanio de pagina para API search")
    parser.add_argument("--max-links", type=int, default=None, help="Limite global de links (0=sin limite)")
    parser.add_argument("--headless", action="store_true", help="Ejecutar navegador en modo headless")
    args = parser.parse_args()

    df = run(
        max_pages=args.max_pages,
        events_per_page=args.events_per_page,
        headless=args.headless,
        max_links=args.max_links,
    )
    if df.empty:
        print("No se extrajeron eventos de Joinnus")
        return

    out_file = ROOT / "output" / "raw" / f"joinnus_preview_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.csv"
    out_file.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_file, index=False, encoding="utf-8-sig")
    print(f"Preview guardado en: {out_file}")


if __name__ == "__main__":
    main()
