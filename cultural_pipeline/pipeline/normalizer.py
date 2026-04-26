"""
normalizer.py
=============
Transforma los datos crudos de cada scraper al esquema unificado
EventoEstandar — el formato canónico para la app de recomendaciones.

Esquema de salida:
    entity_id       : str  — hash único (source + url)
    poi_id          : str  — id estable entre runs para consumo backend/reco
    poi_id_version  : str  — versión del algoritmo de poi_id
    entity_type     : str  — event | place
    event_id        : str  — alias de compatibilidad
    titulo          : str
    descripcion     : str
    tipo            : str  — categoría normalizada
    fecha_inicio    : date (ISO 8601)
    fecha_fin       : date | None
    hora_inicio     : str  | None  — "HH:MM"
    lugar           : str
    direccion       : str  | None
    imagen_url      : str  | None
    precio          : str  — "Gratuito" | "Pago" | "Consultar"
    url_origen      : str
    url_evento      : str  — alias de compatibilidad
    fuente          : str  — "bnp" | "mali" | "joinnus"
    ciudad          : str  — "Lima"
    categoria_normalizada : str — alias canónico de tipo
    geo_hash        : str | None — geohash precisión 7 (≈76m×76m) si hay lat/lng
    fecha_run       : date (ISO 8601) — fecha UTC del run de normalización
    tags            : list[str]
    texto_embedding : str  — concatenación para generar embeddings
    scraped_at      : datetime (ISO 8601)
"""

import hashlib
import re
import ast
import pandas as pd
import pygeohash
from datetime import datetime, date
from typing import Optional


POI_ID_VERSION = "v1"
JOINNUS_DATE_MIN = "2020-01-01"
JOINNUS_DATE_MAX = "2027-12-31"

# ---------------------------------------------------------------------------
# Mapeo de categorías a vocabulario común
# ---------------------------------------------------------------------------
CATEGORY_MAP = {
    # ── Google Places (español snake_case — valores reales del scraper) ────────
    "centro_cultural":       "evento_cultural",
    "galeria_de_arte":       "galeria",
    "iglesia_histórica":     "iglesia",
    "iglesia_historica":     "iglesia",
    "catedral":              "iglesia",
    "mirador":               "mirador",
    "sitio_turístico":       "sitio_turistico",
    "sitio_turistico":       "sitio_turistico",
    "acuario":               "naturaleza",
    "laguna":                "naturaleza",
    "playa":                 "naturaleza",
    "parque_temático":       "parque",
    "parque_tematico":       "parque",
    "parque":                "parque",
    "museo":                 "museo",
    "patrimonio_cultural":   "patrimonio",
    "malecon":               "mirador",
    "malecón":               "mirador",
    "zoologico":             "naturaleza",
    "zoológico":             "naturaleza",
    "sitio_arqueológico":    "patrimonio",
    "sitio_arqueologico":    "patrimonio",
    "plaza_historica":       "patrimonio",
    "parque_de_diversiones": "parque",
    # ── BNP / MALI ─────────────────────────────────────────────────────────────
    "bibliocine":            "cine",
    "charla, conversatorio y/o conferencia": "evento_cultural",
    "conversatorio":         "evento_cultural",
    "taller":                "taller",
    "exposición":            "galeria",
    "exposicion":            "galeria",
    "concierto":             "concierto",
    "teatro":                "teatro",
    "danza":                 "danza",
    "infantil":              "familia",
    "familia":               "familia",
    "recorrido":             "tour",
    "tour":                  "tour",
    "gastronomía":           "gastronomia",
    "gastronomia":           "gastronomia",
    # ── Joinnus ────────────────────────────────────────────────────────────────
    "concerts":              "concierto",
    "exhibitions":           "galeria",
    "theater":               "teatro",
    "gastronomy":            "gastronomia",
    "culture":               "evento_cultural",
    "family":                "familia",
    "sports":                "deporte",
    "workshop":              "taller",
    "comidas & bebidas":     "gastronomia",
    "stand-up":              "teatro",
    "arte & cultura":        "evento_cultural",
    "entertainment":         "evento_cultural",
}


def normalize_category(raw) -> str:
    import math
    if raw is None or (isinstance(raw, float) and math.isnan(raw)):
        return "cultural"
    raw = str(raw).strip()
    if not raw or raw == "nan":
        return "cultural"
    key = raw.lower().strip()
    for pattern, normalized in CATEGORY_MAP.items():
        if pattern in key:
            return normalized
    return key


def make_event_id(source: str, url: str) -> str:
    raw = f"{source}::{url}"
    return hashlib.md5(raw.encode()).hexdigest()[:16]


def _base_record(source: str, url: str, entity_type: str = "event") -> dict:
    entity_id = make_event_id(source, url)
    return {
        "entity_id": entity_id,
        "entity_type": entity_type,
        "event_id": entity_id,
        "url_origen": url,
        "url_evento": url,
    }


def clean_text(text) -> Optional[str]:
    if text is None or (isinstance(text, float) and __import__('math').isnan(text)):
        return None
    text = str(text).strip()
    if not text or text == "nan":
        return None
    text = re.sub(r"\s+", " ", text).strip()
    return text if text else None


def _format_district(text: Optional[str]) -> Optional[str]:
    if not text:
        return None
    out = clean_text(text)
    if not out:
        return None
    out = out.replace("_", " ").replace("-", " ")
    out = re.sub(r"\s+", " ", out).strip()
    return out.title()


def _extract_reviews_summary(raw_reviews, max_reviews: int = 2, max_chars: int = 220) -> Optional[str]:
    if raw_reviews is None:
        return None

    reviews = raw_reviews
    if isinstance(raw_reviews, str):
        raw = raw_reviews.strip()
        if not raw:
            return None
        try:
            reviews = ast.literal_eval(raw)
        except Exception:
            return None

    if not isinstance(reviews, list):
        return None

    snippets = []
    seen = set()
    for item in reviews:
        if not isinstance(item, dict):
            continue
        text = clean_text(item.get("review_text"))
        if not text:
            continue
        if len(text) < 20:
            continue
        key = text.lower()
        if key in seen:
            continue
        seen.add(key)
        snippets.append(text[:max_chars])
        if len(snippets) >= max_reviews:
            break

    if not snippets:
        return None
    return " | ".join(snippets)


def _tags_to_text(raw_tags) -> str:
    if isinstance(raw_tags, list):
        values = raw_tags
    elif isinstance(raw_tags, str):
        values = [t.strip() for t in raw_tags.split(",")]
    elif raw_tags is None:
        values = []
    else:
        values = [raw_tags]

    cleaned = []
    for value in values:
        text = clean_text(value)
        if text:
            cleaned.append(text)
    return ", ".join(cleaned)


def _normalize_token(value) -> str:
    text = clean_text(value)
    if not text:
        return ""
    return text.lower()


def _safe_float(value) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return None
    try:
        return float(text)
    except Exception:
        return None


def _build_geo_hash(lat, lng) -> Optional[str]:
    lat_f = _safe_float(lat)
    lng_f = _safe_float(lng)
    if lat_f is None or lng_f is None:
        return None
    return pygeohash.encode(lat_f, lng_f, precision=7)


def _stable_hash(payload: str) -> str:
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()[:24]


def _compute_poi_id(rec: dict) -> str:
    entity_type = _normalize_token(rec.get("entity_type")) or "event"
    fuente = _normalize_token(rec.get("fuente"))
    categoria = _normalize_token(rec.get("categoria_normalizada"))

    if entity_type == "place":
        place_id = _normalize_token(rec.get("place_id"))
        source_url = _normalize_token(rec.get("url_origen") or rec.get("url_evento"))
        lat_lng = _build_geo_hash(rec.get("lat"), rec.get("lng"))
        title = _normalize_token(rec.get("titulo"))

        stable_key = place_id or source_url or lat_lng or title
        payload = f"{POI_ID_VERSION}|place|{fuente}|{stable_key}|{categoria}"
        return f"poi_{_stable_hash(payload)}"

    source_url = _normalize_token(rec.get("url_evento") or rec.get("url_origen"))
    date_start = _normalize_token(rec.get("fecha_inicio"))
    time_start = _normalize_token(rec.get("hora_inicio"))
    place = _normalize_token(rec.get("lugar"))
    title = _normalize_token(rec.get("titulo"))

    payload = (
        f"{POI_ID_VERSION}|event|{fuente}|{source_url}|"
        f"{date_start}|{time_start}|{place}|{title}|{categoria}"
    )
    return f"poi_{_stable_hash(payload)}"


def _finalize_record(rec: dict) -> dict:
    rec["categoria_normalizada"] = rec.get("tipo") or "cultural"
    rec["geo_hash"] = _build_geo_hash(rec.get("lat"), rec.get("lng"))
    rec["fecha_run"] = datetime.utcnow().date().isoformat()
    rec["poi_id_version"] = POI_ID_VERSION
    rec["poi_id"] = _compute_poi_id(rec)
    return rec


def parse_date(text: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    """Devuelve (fecha_inicio_iso, fecha_fin_iso)"""
    if not text:
        return None, None
    # Formato joinnus: "2025-10-04"
    if re.match(r"\d{4}-\d{2}-\d{2}", str(text)):
        return str(text)[:10], None
    # Formato BNP: "Sábado, 18 de Abril del 2026 6:30PM"
    meses = {
        "enero": "01", "febrero": "02", "marzo": "03",
        "abril": "04", "mayo": "05", "junio": "06",
        "julio": "07", "agosto": "08", "septiembre": "09",
        "octubre": "10", "noviembre": "11", "diciembre": "12"
    }
    m = re.search(r"(\d{1,2}) de (\w+) del? (\d{4})", str(text), re.IGNORECASE)
    if m:
        day, month_name, year = m.groups()
        month = meses.get(month_name.lower())
        if month:
            return f"{year}-{month}-{int(day):02d}", None
    # Formato MALI: "01/04/2026 - 30/06/2026"
    m2 = re.findall(r"(\d{2}/\d{2}/\d{4})", str(text))
    if m2:
        def to_iso(d):
            parts = d.split("/")
            return f"{parts[2]}-{parts[1]}-{parts[0]}"
        start = to_iso(m2[0])
        end = to_iso(m2[1]) if len(m2) > 1 else None
        return start, end
    return None, None


def parse_time(text: Optional[str]) -> Optional[str]:
    if not text:
        return None
    m = re.search(r"(\d{1,2}):(\d{2})\s*(AM|PM)?", str(text), re.IGNORECASE)
    if m:
        h, min_, meridiem = m.groups()
        h = int(h)
        if meridiem and meridiem.upper() == "PM" and h != 12:
            h += 12
        elif meridiem and meridiem.upper() == "AM" and h == 12:
            h = 0
        return f"{h:02d}:{min_}"
    return None


def detect_price(text: Optional[str], etiquetas: Optional[dict] = None) -> str:
    pool = (text or "") + " " + str(etiquetas or "")
    pool = pool.lower()
    if any(w in pool for w in ["gratuito", "libre", "gratis", "free"]):
        return "Gratuito"
    if any(w in pool for w in ["pago", "entrada", "ticket", "s/.", "soles", "pen"]):
        return "Pago"
    return "Consultar"


def build_embedding_text(row: dict) -> str:
    """Crea el texto que será embeddizado — rico en señales semánticas"""
    entity_type = str(row.get("entity_type", "event")).lower()
    if entity_type == "place":
        district = _format_district(row.get("distrito"))
        rating = row.get("rating")
        ratings_total = row.get("ratings_total")
        category = clean_text(row.get("categoria_google") or row.get("tipo"))
        tags = row.get("tags", [])
        if not isinstance(tags, list):
            tags = []

        tags_text = _tags_to_text(tags)
        place_parts = [
            f"Lugar: {row.get('titulo', '')}",
            f"Nombre del lugar: {row.get('titulo', '')}",
            f"Categoría: {category or ''}",
            f"Distrito: {district or ''}",
            f"Dirección: {row.get('direccion', '')}",
            f"Ciudad: {row.get('ciudad', 'Lima')}, Perú",
            f"Descripción: {row.get('descripcion', '')}",
            f"Rating promedio: {rating if rating is not None else ''}",
            f"Cantidad de reseñas: {ratings_total if ratings_total is not None else ''}",
            f"Reseñas destacadas: {row.get('resumen_reviews', '')}",
            f"Precio: {row.get('precio', '')}",
            f"Tags: {tags_text}",
        ]
        return " | ".join(p for p in place_parts if p.split(": ", 1)[1])

    label = "Evento"
    tags_text = _tags_to_text(row.get("tags", []))
    parts = [
        f"{label}: {row.get('titulo', '')}",
        f"Tipo: {row.get('tipo', '')}",
        f"Lugar: {row.get('lugar', '')}",
        f"Ciudad: Lima, Perú",
        f"Descripción: {row.get('descripcion', '')}",
        f"Precio: {row.get('precio', '')}",
        f"Tags: {tags_text}",
    ]
    return " | ".join(p for p in parts if p.split(": ", 1)[1])


def _titulo_from_url(url: str) -> Optional[str]:
    """Extrae un título legible desde la URL cuando el scraper no lo capturó"""
    try:
        slug = url.rstrip("/").split("/")[-1]
        # Quitar el sufijo numérico tipo -5902
        slug = re.sub(r"-\d+$", "", slug)
        return slug.replace("-", " ").title()
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Normalizadores por fuente
# ---------------------------------------------------------------------------

def normalize_bnp(df_raw: pd.DataFrame) -> pd.DataFrame:
    records = []
    for _, row in df_raw.iterrows():
        etiquetas = row.get("etiquetas") if isinstance(row.get("etiquetas"), dict) else {}

        # Lugar: puede estar en varias columnas aplanadas
        lugar = (
            row.get("etiquetas.Sede")
            or etiquetas.get("Sede")
            or "Biblioteca Nacional del Perú"
        )

        info_text = str(row.get("info_adicional", ""))
        fecha_inicio, fecha_fin = parse_date(info_text)
        hora = parse_time(info_text)

        # Tags semánticos
        tags = [row.get("tipo", ""), lugar]
        tags = [t for t in tags if t]

        rec = _base_record("bnp", str(row.get("url", "")), entity_type="event")
        rec.update({
            "titulo": clean_text(row.get("titulo")) or _titulo_from_url(str(row.get("url", ""))),
            "descripcion": clean_text(row.get("descripcion")),
            "tipo": normalize_category(row.get("tipo")),
            "fecha_inicio": fecha_inicio,
            "fecha_fin": fecha_fin,
            "hora_inicio": hora,
            "lugar": lugar,
            "direccion": None,
            "imagen_url": row.get("imagen"),
            "precio": detect_price(
                str(row.get("etiquetas.Ingreso", "")) + str(row.get("estado", ""))
            ),
            "url_origen": row.get("url"),
            "url_evento": row.get("url"),
            "fuente": "bnp",
            "ciudad": "Lima",
            "tags": tags,
            "scraped_at": row.get("_scraped_at", datetime.utcnow().isoformat()),
        })
        rec = _finalize_record(rec)
        rec["texto_embedding"] = build_embedding_text(rec)
        records.append(rec)
    return pd.DataFrame(records)


def normalize_mali(df_raw: pd.DataFrame) -> pd.DataFrame:
    records = []
    for _, row in df_raw.iterrows():
        fecha_inicio, fecha_fin = parse_date(row.get("fecha") or row.get("fecha_lista"))
        hora = parse_time(row.get("hora"))
        lugar = row.get("lugar") or "MALI - Museo de Arte de Lima"

        tags = [row.get("tipo", ""), lugar, "museo", "arte"]
        tags = [t for t in tags if t]

        rec = _base_record("mali", str(row.get("url", "")), entity_type="event")
        rec.update({
            "titulo": clean_text(row.get("titulo")),
            "descripcion": clean_text(row.get("descripcion")),
            "tipo": normalize_category(row.get("tipo")),
            "fecha_inicio": fecha_inicio,
            "fecha_fin": fecha_fin,
            "hora_inicio": hora,
            "lugar": lugar,
            "direccion": "Paseo Colón 125, Lima",
            "imagen_url": row.get("imagen"),
            "precio": detect_price(row.get("descripcion")),
            "url_origen": row.get("url"),
            "url_evento": row.get("url"),
            "fuente": "mali",
            "ciudad": "Lima",
            "tags": tags,
            "scraped_at": row.get("_scraped_at", datetime.utcnow().isoformat()),
        })
        rec = _finalize_record(rec)
        rec["texto_embedding"] = build_embedding_text(rec)
        records.append(rec)
    return pd.DataFrame(records)


def normalize_joinnus(df_raw: pd.DataFrame) -> pd.DataFrame:
    records = []
    for _, row in df_raw.iterrows():
        fecha_inicio, fecha_fin = parse_date(row.get("date"))
        hora = parse_time(str(row.get("time", "")))

        loc_raw = str(row.get("location", ""))
        # Joinnus location es texto libre tipo "Peru, San Isidro, Lima, ..."
        lugar = loc_raw.split(",")[1].strip() if "," in loc_raw else loc_raw

        tags = [row.get("category", ""), lugar]
        performers = row.get("performer_list")
        if performers and str(performers) != "nan":
            tags += [p.strip() for p in str(performers).split(";")]
        tags = [t for t in tags if t and str(t) != "nan"]

        origen = str(row.get("canonical_url", row.get("source_url", "")))
        rec = _base_record("joinnus", origen, entity_type="event")
        rec.update({
            "titulo": clean_text(row.get("title")),
            "descripcion": clean_text(row.get("description")),
            "tipo": normalize_category(row.get("category")),
            "fecha_inicio": fecha_inicio,
            "fecha_fin": fecha_fin,
            "hora_inicio": hora,
            "lugar": lugar,
            "direccion": None,
            "imagen_url": row.get("image_url"),
            "precio": "Consultar" if str(row.get("ticket_availability", "")) == "available" else "Gratuito",
            "url_origen": row.get("canonical_url") or row.get("source_url"),
            "url_evento": row.get("canonical_url") or row.get("source_url"),
            "fuente": "joinnus",
            "ciudad": "Lima",
            "tags": tags,
            "scraped_at": row.get("_scraped_at", datetime.utcnow().isoformat()),
        })
        rec = _finalize_record(rec)
        rec["texto_embedding"] = build_embedding_text(rec)
        records.append(rec)
    return pd.DataFrame(records)


def normalize_places(df_raw: pd.DataFrame) -> pd.DataFrame:
    records = []
    for _, row in df_raw.iterrows():
        source_url = clean_text(row.get("maps_url") or row.get("url") or row.get("website"))
        place_id = clean_text(row.get("place_id"))
        unique_key = source_url or place_id or clean_text(row.get("name")) or ""

        rec = _base_record("google_places", unique_key, entity_type="place")

        place_name = clean_text(row.get("name") or row.get("title"))
        category = clean_text(row.get("category") or row.get("categoria") or row.get("primary_type"))
        district = _format_district(row.get("distrito"))
        rating = row.get("rating")
        reviews = row.get("user_ratings_total") or row.get("reviews_total")
        reviews_summary = _extract_reviews_summary(row.get("reviews"))

        tags = []
        raw_types = row.get("types")
        if isinstance(raw_types, list):
            tags.extend([str(t).strip() for t in raw_types if str(t).strip()])
        elif raw_types and str(raw_types) != "nan":
            tags.extend([t.strip() for t in str(raw_types).split(",") if t.strip()])
        if category:
            tags.append(category)
        if district:
            tags.append(district)

        description = clean_text(row.get("description")) or clean_text(row.get("editorial_summary"))
        if not description:
            geo_hint = f" en {district}" if district else " en Lima"
            cat_hint = f" de tipo {category}" if category else ""
            rating_hint = f" con rating {rating}" if rating not in (None, "", "nan") else ""
            description = f"Lugar{geo_hint}{cat_hint}{rating_hint}".strip()

        rec.update({
            "titulo": place_name,
            "descripcion": description,
            "tipo": normalize_category(category or "place"),
            "fecha_inicio": None,
            "fecha_fin": None,
            "hora_inicio": None,
            "lugar": place_name,
            "direccion": clean_text(row.get("formatted_address") or row.get("address")),
            "imagen_url": clean_text(row.get("image_url") or row.get("photo_url")),
            "precio": "Consultar",
            "url_origen": source_url,
            "url_evento": source_url,
            "fuente": "google_places",
            "ciudad": clean_text(row.get("city")) or "Lima",
            "tags": list(dict.fromkeys([t for t in tags if t and t != "nan"])),
            "scraped_at": row.get("_scraped_at", datetime.utcnow().isoformat()),
            "place_id": place_id,
            "rating": rating,
            "ratings_total": reviews,
            "lat": row.get("lat") or row.get("latitude"),
            "lng": row.get("lng") or row.get("longitude"),
            "categoria_google": category,
            "distrito": district,
            "resumen_reviews": reviews_summary,
        })
        rec = _finalize_record(rec)
        rec["texto_embedding"] = build_embedding_text(rec)
        records.append(rec)
    return pd.DataFrame(records)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def normalize_all(
    bnp_df: pd.DataFrame = None,
    mali_df: pd.DataFrame = None,
    joinnus_df: pd.DataFrame = None,
    places_df: pd.DataFrame = None,
) -> pd.DataFrame:
    frames = []
    if bnp_df is not None and not bnp_df.empty:
        frames.append(normalize_bnp(bnp_df))
    if mali_df is not None and not mali_df.empty:
        frames.append(normalize_mali(mali_df))
    if joinnus_df is not None and not joinnus_df.empty:
        frames.append(normalize_joinnus(joinnus_df))
    if places_df is not None and not places_df.empty:
        frames.append(normalize_places(places_df))

    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    # Deduplicar por entity_id (compatibilidad con event_id)
    key = "entity_id" if "entity_id" in combined.columns else "event_id"
    combined = combined.drop_duplicates(subset=[key])

    # Deduplicacion semantica conservadora para Joinnus.
    # Evita near-duplicates cuando un mismo evento aparece con multiples URLs
    # pero comparte los mismos atributos canónicos de negocio.
    required_cols = {"entity_type", "fuente", "titulo", "fecha_inicio", "hora_inicio", "lugar", "tipo"}
    if required_cols.issubset(set(combined.columns)):
        joinnus_mask = (
            combined["entity_type"].astype(str).str.lower().eq("event")
            & combined["fuente"].astype(str).str.lower().eq("joinnus")
        )
        if bool(joinnus_mask.any()):
            joinnus_df = combined.loc[joinnus_mask].copy()
            joinnus_df["_titulo_norm"] = (
                joinnus_df["titulo"].fillna("").astype(str).str.lower().str.replace(r"\s+", " ", regex=True).str.strip()
            )
            joinnus_df["_lugar_norm"] = (
                joinnus_df["lugar"].fillna("").astype(str).str.lower().str.replace(r"\s+", " ", regex=True).str.strip()
            )
            joinnus_df["_tipo_norm"] = (
                joinnus_df["tipo"].fillna("").astype(str).str.lower().str.replace(r"\s+", " ", regex=True).str.strip()
            )
            joinnus_df["_fecha_norm"] = joinnus_df["fecha_inicio"].fillna("").astype(str).str.strip()
            joinnus_df["_hora_norm"] = joinnus_df["hora_inicio"].fillna("").astype(str).str.strip()
            joinnus_df["_desc_len"] = joinnus_df.get("descripcion", "").fillna("").astype(str).str.len()
            joinnus_df["_semantic_key"] = (
                joinnus_df["_titulo_norm"]
                + "||"
                + joinnus_df["_fecha_norm"]
                + "||"
                + joinnus_df["_hora_norm"]
                + "||"
                + joinnus_df["_lugar_norm"]
                + "||"
                + joinnus_df["_tipo_norm"]
            )

            before = len(joinnus_df)
            joinnus_df = joinnus_df.sort_values(["_semantic_key", "_desc_len"], ascending=[True, False])
            joinnus_df = joinnus_df.drop_duplicates(subset=["_semantic_key"], keep="first")
            dropped = before - len(joinnus_df)

            if dropped > 0:
                combined = pd.concat([combined.loc[~joinnus_mask], joinnus_df[combined.columns]], ignore_index=True)
                print(f"🔧 Deduplicacion semantica Joinnus: -{dropped} registros")

    # Filtrar eventos Joinnus con fechas anómalas o título+fecha duplicados
    jn_event_mask = (
        combined["entity_type"].astype(str).str.lower().eq("event")
        & combined["fuente"].astype(str).str.lower().eq("joinnus")
    )
    if jn_event_mask.any():
        jn = combined.loc[jn_event_mask].copy()
        date_str = jn["fecha_inicio"].fillna("").astype(str)
        has_date = date_str.str.match(r"\d{4}-\d{2}-\d{2}")
        invalid_date = has_date & ((date_str < JOINNUS_DATE_MIN) | (date_str > JOINNUS_DATE_MAX))
        if invalid_date.any():
            print(f"🗑️  Joinnus fechas fuera de rango [{JOINNUS_DATE_MIN}, {JOINNUS_DATE_MAX}]: -{invalid_date.sum()} registros")
            jn = jn[~invalid_date]
        titulo_fecha_key = (
            jn["titulo"].fillna("").astype(str).str.lower().str.strip()
            + "||"
            + jn["fecha_inicio"].fillna("").astype(str).str.strip()
        )
        dupes = titulo_fecha_key.duplicated(keep="first")
        if dupes.any():
            print(f"🗑️  Joinnus título+fecha duplicados: -{dupes.sum()} registros")
            jn = jn[~dupes]
        combined = pd.concat([combined.loc[~jn_event_mask], jn], ignore_index=True)

    # Ordenar
    combined = combined.sort_values("fecha_inicio", na_position="last")
    print(f"📦 Total eventos normalizados: {len(combined)}")
    return combined
