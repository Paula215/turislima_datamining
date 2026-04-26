"""
Schema Parquet tipado para la capa Silver (`EventoEstandar`).

Cualquier cambio incompatible aquí debe bumpear `SILVER_SCHEMA_VERSION`
(semver). Los validadores y consumidores leen este versionado para
detectar drift.
"""

from __future__ import annotations

import pyarrow as pa


SILVER_SCHEMA_VERSION = "1.0.0"


SILVER_REQUIRED_COLUMNS: tuple[str, ...] = (
    "entity_id",
    "entity_type",
    "titulo",
    "fuente",
    "url_origen",
    "texto_embedding",
    "scraped_at",
    "fecha_run",
)


SILVER_CRITICAL_NULL_COLUMNS: tuple[str, ...] = (
    "entity_id",
    "entity_type",
    "titulo",
    "fuente",
    "texto_embedding",
)


def silver_arrow_schema() -> pa.Schema:
    """Schema canónico de Silver (eventos_estandar.parquet)."""
    return pa.schema(
        [
            ("entity_id", pa.string()),
            ("entity_type", pa.string()),
            ("event_id", pa.string()),
            ("poi_id", pa.string()),
            ("poi_id_version", pa.string()),
            ("titulo", pa.string()),
            ("descripcion", pa.string()),
            ("tipo", pa.string()),
            ("categoria_normalizada", pa.string()),
            ("fecha_inicio", pa.date32()),
            ("fecha_fin", pa.date32()),
            ("hora_inicio", pa.string()),
            ("lugar", pa.string()),
            ("direccion", pa.string()),
            ("distrito", pa.string()),
            ("ciudad", pa.string()),
            ("imagen_url", pa.string()),
            ("precio", pa.string()),
            ("url_origen", pa.string()),
            ("url_evento", pa.string()),
            ("fuente", pa.string()),
            ("tags", pa.list_(pa.string())),
            ("geo_hash", pa.string()),
            ("lat", pa.float64()),
            ("lng", pa.float64()),
            ("place_id", pa.string()),
            ("rating", pa.float64()),
            ("ratings_total", pa.int64()),
            ("categoria_google", pa.string()),
            ("resumen_reviews", pa.string()),
            ("texto_embedding", pa.string()),
            ("fecha_run", pa.date32()),
            ("scraped_at", pa.timestamp("us", tz="UTC")),
            ("schema_version", pa.string()),
        ],
        metadata={
            b"schema_version": SILVER_SCHEMA_VERSION.encode("utf-8"),
            b"dataset": b"eventos_estandar",
            b"layer": b"silver",
        },
    )
