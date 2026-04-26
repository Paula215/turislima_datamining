"""
Contratos formales entre las capas Bronze/Silver/Gold del medallón.

Las capas se comunican por:
  - Schemas Parquet tipados (silver_schema, gold_schema).
  - Manifests JSON por run (manifests).
  - Convenciones de paths en el lake (layout).

Cualquier consumidor (validadores, sinks, app) debe importar desde aquí
en lugar de hard-codear nombres de columnas o rutas.
"""

from .layout import (
    LakeLayout,
    bronze_path,
    silver_path,
    gold_catalog_path,
    gold_vectors_path,
    manifest_path,
)
from .manifests import (
    BronzeManifest,
    BronzeSourceEntry,
    SilverManifest,
    GoldManifest,
)
from .silver_schema import (
    SILVER_SCHEMA_VERSION,
    silver_arrow_schema,
    SILVER_REQUIRED_COLUMNS,
    SILVER_CRITICAL_NULL_COLUMNS,
)
from .gold_schema import (
    GOLD_SCHEMA_VERSION,
    gold_catalog_arrow_schema,
    gold_vectors_arrow_schema,
    EMBEDDING_DIM,
)

__all__ = [
    "LakeLayout",
    "bronze_path",
    "silver_path",
    "gold_catalog_path",
    "gold_vectors_path",
    "manifest_path",
    "BronzeManifest",
    "BronzeSourceEntry",
    "SilverManifest",
    "GoldManifest",
    "SILVER_SCHEMA_VERSION",
    "silver_arrow_schema",
    "SILVER_REQUIRED_COLUMNS",
    "SILVER_CRITICAL_NULL_COLUMNS",
    "GOLD_SCHEMA_VERSION",
    "gold_catalog_arrow_schema",
    "gold_vectors_arrow_schema",
    "EMBEDDING_DIM",
]
