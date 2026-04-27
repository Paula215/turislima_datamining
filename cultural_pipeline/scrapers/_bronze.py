"""
Helpers compartidos para volcar el payload crudo de cada scraper a la
capa Bronze del lake.

Cada scraper expone `run_with_payload() -> ScrapeResult`. El stage
`bronze.py` del pipeline (o cualquier consumidor) toma ese resultado y
lo escribe en el lake mediante `dump_to_bronze`.

Diseño:
  - Los scrapers no conocen el lake (no importan `pipeline.storage`).
    Solo construyen el ScrapeResult.
  - `dump_to_bronze` recibe un `BaseLakeStore` y persiste:
      * events.jsonl: un dict por línea, los `raw_records` capturados.
      * _source_manifest.json: metadata específica de la fuente.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import pandas as pd

from contracts import BronzeSourceEntry, bronze_path  # type: ignore[import-not-found]


@dataclass
class ScrapeResult:
    """Resultado canónico de un scraper.

    Attributes:
        df: el DataFrame parseado que consume el normalizer (Silver).
        raw_records: lista de dicts pre-DataFrame; payload reprocesable.
        metadata: información de trazabilidad (scraper_version,
            ingest_ts, http_status, urls_visited, notes, etc.).
    """

    df: pd.DataFrame
    raw_records: list[dict[str, Any]] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def dump_to_bronze(
    store,  # BaseLakeStore — type hint relajado para evitar import circular
    source: str,
    run_id: str,
    result: ScrapeResult,
) -> BronzeSourceEntry:
    """Persiste el ScrapeResult en Bronze y devuelve la entrada de manifest.

    Layout escrito:
        bronze/source=<source>/run_id=<run_id>/events.jsonl
        bronze/source=<source>/run_id=<run_id>/_source_manifest.json
    """
    payload_path = bronze_path(source, run_id, "events.jsonl")
    body = "\n".join(
        json.dumps(rec, ensure_ascii=False, default=str) for rec in result.raw_records
    )
    store.write_bronze(source, run_id, body.encode("utf-8"), filename="events.jsonl")

    source_manifest_path = bronze_path(source, run_id, "_source_manifest.json")
    source_manifest = {
        "source": source,
        "run_id": run_id,
        "ingest_ts": result.metadata.get("ingest_ts", _utcnow_iso()),
        "scraper_version": result.metadata.get("scraper_version", "unknown"),
        "row_count": len(result.raw_records),
        "df_row_count": int(len(result.df)),
        **{
            k: v
            for k, v in result.metadata.items()
            if k not in ("ingest_ts", "scraper_version")
        },
    }
    store.write_bronze(
        source,
        run_id,
        json.dumps(source_manifest, ensure_ascii=False, indent=2).encode("utf-8"),
        filename="_source_manifest.json",
    )

    return BronzeSourceEntry(
        name=source,
        scraper_version=source_manifest["scraper_version"],
        payload_path=payload_path,
        http_status=result.metadata.get("http_status"),
        ingest_ts=source_manifest["ingest_ts"],
        row_count_estimate=len(result.raw_records),
        url=result.metadata.get("url"),
        notes=result.metadata.get("notes"),
    )
