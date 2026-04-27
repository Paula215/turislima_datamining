"""
Stage Silver: normaliza los DataFrames crudos y produce el dataset
canónico `EventoEstandar` en Parquet tipado, además del CSV/JSON legacy.

Entradas:
  - dict[source -> ScrapeResult] (en memoria, desde Bronze stage), o
  - los DataFrames raw equivalentes leídos de Bronze (futuro).

Salidas:
  - lake://silver/eventos_estandar/run_id=<id>/part-0000.parquet
  - lake://silver/_manifest_<id>.json
  - lake://silver/_latest.json
  - output/eventos_estandar.csv  (legacy, app + validadores)
  - output/eventos_estandar.json (legacy, API)
  - output/snapshots/eventos_<id>.csv (legacy, snapshot histórico)
  - logs/stats_<id>.json (legacy, telemetría)
"""

from __future__ import annotations

import json
import logging
import traceback
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pyarrow as pa

from contracts import (  # type: ignore[import-not-found]
    SILVER_SCHEMA_VERSION,
    SilverManifest,
    silver_arrow_schema,
)


log = logging.getLogger(__name__)


def _df_to_silver_table(df: pd.DataFrame) -> pa.Table:
    """Coerce un DataFrame de EventoEstandar al schema Silver tipado.

    Las columnas faltantes se rellenan con None; las extra se descartan.
    """
    schema = silver_arrow_schema()
    out = {}
    for field in schema:
        col = field.name
        if col == "schema_version":
            out[col] = [SILVER_SCHEMA_VERSION] * len(df)
        elif col in df.columns:
            out[col] = df[col]
        else:
            out[col] = [None] * len(df)
    df_aligned = pd.DataFrame(out)

    # Coerciones explícitas para tipos no-string
    if "fecha_inicio" in df_aligned.columns:
        df_aligned["fecha_inicio"] = pd.to_datetime(
            df_aligned["fecha_inicio"], errors="coerce"
        ).dt.date
    if "fecha_fin" in df_aligned.columns:
        df_aligned["fecha_fin"] = pd.to_datetime(
            df_aligned["fecha_fin"], errors="coerce"
        ).dt.date
    if "fecha_run" in df_aligned.columns:
        df_aligned["fecha_run"] = pd.to_datetime(
            df_aligned["fecha_run"], errors="coerce"
        ).dt.date
    if "scraped_at" in df_aligned.columns:
        df_aligned["scraped_at"] = pd.to_datetime(
            df_aligned["scraped_at"], errors="coerce", utc=True
        )

    return pa.Table.from_pandas(df_aligned, schema=schema, preserve_index=False)


def _save_legacy(df: pd.DataFrame, run_id: str, output_dir: Path, logs_dir: Path) -> None:
    """Mantiene los outputs legacy (CSV/JSON/snapshot/stats) intactos."""
    output_dir.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)

    csv_path = output_dir / "eventos_estandar.csv"
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    log.info("📄 CSV legacy guardado: %s", csv_path)

    json_path = output_dir / "eventos_estandar.json"
    records = df.to_dict(orient="records")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2, default=str)
    log.info("📄 JSON legacy guardado: %s", json_path)

    snapshot_dir = output_dir / "snapshots"
    snapshot_dir.mkdir(exist_ok=True)
    df.to_csv(
        snapshot_dir / f"eventos_{run_id}.csv",
        index=False,
        encoding="utf-8-sig",
    )

    stats = {
        "run_id": run_id,
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "total_eventos": len(df),
        "por_entity_type": df["entity_type"].value_counts().to_dict() if "entity_type" in df.columns else {},
        "por_fuente": df["fuente"].value_counts().to_dict() if "fuente" in df.columns else {},
        "por_tipo": df["tipo"].value_counts().to_dict() if "tipo" in df.columns else {},
        "por_precio": df["precio"].value_counts().to_dict() if "precio" in df.columns else {},
    }
    with open(logs_dir / f"stats_{run_id}.json", "w", encoding="utf-8") as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)


def run(
    run_id: str,
    raw_dfs: dict[str, pd.DataFrame],
    store,
    output_dir: Path,
    logs_dir: Path,
    do_geocoding: bool = True,
) -> pd.DataFrame:
    """Normaliza, geocodifica y persiste Silver + legacy outputs."""
    log.info("🥈 Silver stage iniciado — run_id=%s", run_id)

    from normalizer import normalize_all  # type: ignore[import-not-found]

    df = normalize_all(
        bnp_df=raw_dfs.get("bnp"),
        mali_df=raw_dfs.get("mali"),
        joinnus_df=raw_dfs.get("joinnus"),
        places_df=raw_dfs.get("places"),
    )

    if df.empty:
        log.warning("⚠️ Silver vacío — los scrapers no produjeron eventos")
        return df

    # Geocoding (rellena lat/lng/geo_hash sin coordenadas)
    if do_geocoding and "lat" in df.columns and df["lat"].isna().any():
        try:
            from geocoder import geocode_events  # type: ignore[import-not-found]
            df = geocode_events(df, run_id=run_id)
        except Exception as e:
            log.error("❌ Error en geocoding: %s\n%s", e, traceback.format_exc())
            log.warning("Continuando sin geocoding.")

    # Lake: Silver Parquet + manifest
    table = _df_to_silver_table(df)
    silver_path = store.write_silver(run_id, table)
    log.info("📦 Silver Parquet: %s (%s filas, %s cols)",
             silver_path, table.num_rows, table.num_columns)

    sources_count = (
        df["fuente"].value_counts().to_dict() if "fuente" in df.columns else {}
    )
    silver_manifest = SilverManifest(
        run_id=run_id,
        schema_version=SILVER_SCHEMA_VERSION,
        parquet_path=silver_path,
        row_count=int(len(df)),
        sources={str(k): int(v) for k, v in sources_count.items()},
        dq_checks={
            "dup_entity_id": int(
                len(df) - df["entity_id"].nunique(dropna=True)
            ) if "entity_id" in df.columns else -1,
            "null_titulo_ratio": float(
                df["titulo"].isna().mean()
            ) if "titulo" in df.columns else -1.0,
        },
    )
    store.write_manifest("silver", run_id, silver_manifest)

    # Legacy outputs (no romper consumidores actuales)
    _save_legacy(df, run_id, output_dir, logs_dir)

    return df
