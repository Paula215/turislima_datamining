"""
catalog_exporter.py
===================
Genera poi_catalog.json: catálogo canónico de POIs con embeddings,
para consumo directo del backend de recomendaciones.

Esquema por registro:
    poi_id               str   — identificador estable entre runs
    nombre               str   — título del lugar/evento
    lat                  float | null
    lon                  float | null
    geo_hash             str | null  — geohash precisión 7
    categoria_normalizada str
    rating               float | null
    fuente               str   — bnp | mali | joinnus | google_places
    fecha_run            str   — ISO 8601
    embedding            list[float] x384 | null
"""

import json
import numpy as np
import pandas as pd
from pathlib import Path
from typing import Optional


CATALOG_FILENAME = "poi_catalog.json"


def _safe_float(value) -> Optional[float]:
    if value is None:
        return None
    try:
        f = float(value)
        return None if (f != f) else f  # NaN → None
    except Exception:
        return None


def export_poi_catalog(
    df: pd.DataFrame,
    embeddings: Optional[np.ndarray],
    output_dir: Path,
) -> Path:
    """
    Escribe poi_catalog.json en output_dir.

    embeddings puede ser None (cuando se omiten con --skip-embeddings);
    en ese caso el campo embedding queda null en cada registro.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(exist_ok=True)

    has_embeddings = (
        embeddings is not None
        and isinstance(embeddings, np.ndarray)
        and embeddings.ndim == 2
        and len(embeddings) == len(df)
    )

    records = []
    for i, (_, row) in enumerate(df.iterrows()):
        records.append({
            "poi_id": str(row.get("poi_id") or ""),
            "nombre": str(row.get("titulo") or ""),
            "lat": _safe_float(row.get("lat")),
            "lon": _safe_float(row.get("lng")),
            "geo_hash": row.get("geo_hash") or None,
            "categoria_normalizada": str(row.get("categoria_normalizada") or "cultural"),
            "rating": _safe_float(row.get("rating")),
            "fuente": str(row.get("fuente") or ""),
            "fecha_run": str(row.get("fecha_run") or ""),
            "embedding": embeddings[i].tolist() if has_embeddings else None,
        })

    out_path = output_dir / CATALOG_FILENAME
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, separators=(",", ":"))

    print(f"📖 poi_catalog.json escrito: {out_path}  ({len(records)} registros)")
    return out_path
