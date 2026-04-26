"""
Schemas Parquet para la capa Gold (catálogo + vectores).

- `catalog_eventos`: subset de Silver pensado para el catálogo de la app
  (sin embeddings, sin texto_embedding).
- `vectors`: tabla con (entity_id, embedding[384], texto) para upsert al
  índice HNSW de Cosmos vCore.
"""

from __future__ import annotations

import pyarrow as pa


GOLD_SCHEMA_VERSION = "1.0.0"
EMBEDDING_DIM = 384


def gold_catalog_arrow_schema() -> pa.Schema:
    """Catálogo curado para la app web (sin vectores)."""
    return pa.schema(
        [
            ("entity_id", pa.string()),
            ("entity_type", pa.string()),
            ("poi_id", pa.string()),
            ("titulo", pa.string()),
            ("descripcion", pa.string()),
            ("tipo", pa.string()),
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
            ("fuente", pa.string()),
            ("tags", pa.list_(pa.string())),
            ("geo_hash", pa.string()),
            ("lat", pa.float64()),
            ("lng", pa.float64()),
            ("rating", pa.float64()),
            ("fecha_run", pa.date32()),
            ("schema_version", pa.string()),
        ],
        metadata={
            b"schema_version": GOLD_SCHEMA_VERSION.encode("utf-8"),
            b"dataset": b"catalog_eventos",
            b"layer": b"gold",
        },
    )


def gold_vectors_arrow_schema() -> pa.Schema:
    """Tabla de vectores lista para upsert a Cosmos vCore."""
    return pa.schema(
        [
            ("entity_id", pa.string()),
            ("titulo", pa.string()),
            ("tipo", pa.string()),
            ("fuente", pa.string()),
            ("fecha_inicio", pa.date32()),
            ("texto_embedding", pa.string()),
            ("embedding", pa.list_(pa.float32(), list_size=EMBEDDING_DIM)),
            ("model_name", pa.string()),
            ("schema_version", pa.string()),
        ],
        metadata={
            b"schema_version": GOLD_SCHEMA_VERSION.encode("utf-8"),
            b"dataset": b"vectors",
            b"layer": b"gold",
            b"embedding_dim": str(EMBEDDING_DIM).encode("utf-8"),
        },
    )
