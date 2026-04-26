"""
Manifests JSON emitidos al final de cada stage.

Cada manifest es la verdad oficial de un run en su capa: contiene
trazabilidad (run_id, versiones), métricas básicas y punteros a los
artefactos producidos. Reemplaza a los `contract_*.json` actuales.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


@dataclass
class BronzeSourceEntry:
    name: str
    scraper_version: str
    payload_path: str
    http_status: int | None
    ingest_ts: str
    row_count_estimate: int | None = None
    url: str | None = None
    notes: str | None = None


@dataclass
class BronzeManifest:
    run_id: str
    layer: str = "bronze"
    schema_version: str = "1.0.0"
    created_at: str = field(default_factory=_utcnow_iso)
    sources: list[BronzeSourceEntry] = field(default_factory=list)


@dataclass
class SilverManifest:
    run_id: str
    schema_version: str
    parquet_path: str
    row_count: int
    sources: dict[str, int]
    layer: str = "silver"
    created_at: str = field(default_factory=_utcnow_iso)
    dq_checks: dict[str, float | int | bool] = field(default_factory=dict)


@dataclass
class GoldManifest:
    run_id: str
    schema_version: str
    catalog_path: str
    vectors_path: str
    catalog_count: int
    vector_count: int
    embedding_dim: int
    model_name: str
    cosmos_db_catalog: str | None = None
    cosmos_db_reco: str | None = None
    layer: str = "gold"
    created_at: str = field(default_factory=_utcnow_iso)


def to_json(manifest: BronzeManifest | SilverManifest | GoldManifest) -> str:
    """Serializa un manifest preservando dataclasses anidadas."""
    return json.dumps(asdict(manifest), ensure_ascii=False, indent=2)
