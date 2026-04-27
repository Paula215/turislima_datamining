"""
Valida la capa Silver de un run.

Lee el Parquet `silver/eventos_estandar/run_id=<id>/part-*.parquet`
producido por el stage `silver` y verifica:
  - schema_version coincide con SILVER_SCHEMA_VERSION
  - columnas requeridas presentes
  - duplicados de entity_id
  - null ratios en columnas críticas
  - cobertura por fuente esperada / drop ratio vs run anterior
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
LOGS = ROOT / "logs"

sys.path.insert(0, str(ROOT / "pipeline"))
sys.path.insert(0, str(ROOT / "scrapers"))

from contracts import (  # type: ignore[import-not-found]
    SILVER_CRITICAL_NULL_COLUMNS,
    SILVER_REQUIRED_COLUMNS,
    SILVER_SCHEMA_VERSION,
    manifest_path,
    silver_path,
)
from storage import get_store  # type: ignore[import-not-found]


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _format_ratio(value: float) -> str:
    return f"{(value * 100):.1f}%"


def _normalize_source(source: str) -> str:
    alias = {"places": "google_places"}
    key = str(source or "").strip().lower()
    return alias.get(key, key)


def _resolve_run_id(store, run_id: str) -> str:
    if run_id != "latest":
        return run_id
    latest = store.get_latest_run_id("silver")
    if not latest:
        raise FileNotFoundError(
            "Capa Silver sin _latest.json — corre `--stage=silver` o `--stage=all` primero"
        )
    return latest


def _read_prior_silver_manifest(store, current_run_id: str) -> dict[str, Any] | None:
    """Lee la lista de runs Silver disponibles y devuelve el manifest del
    run anterior al actual (si existe)."""
    runs = sorted(store.list_run_ids("silver", "eventos_estandar"))
    if current_run_id not in runs:
        return None
    idx = runs.index(current_run_id)
    if idx == 0:
        return None
    prior = runs[idx - 1]
    p = manifest_path("silver", prior)
    if not store.exists(p):
        return None
    return json.loads(store.read_bytes(p).decode("utf-8"))


def validate_silver(
    store,
    run_id: str,
    expected_sources: list[str] | None,
    max_null_ratio: float,
    warn_drop_ratio: float,
) -> dict[str, Any]:
    failures: list[str] = []
    warnings: list[str] = []

    parquet_p = silver_path(run_id)
    if not store.exists(parquet_p):
        failures.append(f"Silver Parquet no existe: {parquet_p}")
        return _build_report(run_id, failures, warnings)

    table = store.read_parquet(parquet_p)
    df = table.to_pandas()

    # Schema version (en metadata del schema arrow)
    schema_meta = dict(table.schema.metadata or {})
    sv = schema_meta.get(b"schema_version")
    schema_version = sv.decode("utf-8") if isinstance(sv, bytes) else None
    if schema_version != SILVER_SCHEMA_VERSION:
        warnings.append(
            f"schema_version del Parquet={schema_version!r} != contrato {SILVER_SCHEMA_VERSION!r}"
        )

    # Required columns
    missing_cols = [c for c in SILVER_REQUIRED_COLUMNS if c not in df.columns]
    if missing_cols:
        failures.append(f"Columnas requeridas ausentes: {missing_cols}")

    # Empty
    total_rows = len(df)
    if total_rows == 0:
        failures.append("Silver Parquet vacío")
        return _build_report(run_id, failures, warnings, total_rows=0)

    # entity_id duplicates
    unique_eids = int(df["entity_id"].nunique(dropna=True)) if "entity_id" in df.columns else 0
    dupes = total_rows - unique_eids
    if dupes > 0:
        failures.append(f"{dupes} entity_id duplicados")

    # Null ratios
    null_ratios: dict[str, float] = {}
    for col in SILVER_CRITICAL_NULL_COLUMNS:
        if col not in df.columns:
            continue
        ratio = float(df[col].isna().mean())
        null_ratios[col] = ratio
        if ratio > max_null_ratio:
            warnings.append(
                f"Nulos altos en {col}: {_format_ratio(ratio)} (umbral {_format_ratio(max_null_ratio)})"
            )

    # Source counts
    source_counts: dict[str, int] = {}
    if "fuente" in df.columns:
        source_counts = {str(k): int(v) for k, v in df["fuente"].value_counts(dropna=False).items()}

    if expected_sources:
        normalized = [_normalize_source(s) for s in expected_sources]
        missing_sources = [s for s in normalized if source_counts.get(s, 0) == 0]
        if missing_sources:
            warnings.append(f"Fuentes esperadas sin registros: {missing_sources}")

    # Coverage drop vs run anterior (lee SilverManifest del run previo)
    source_drops: list[dict[str, Any]] = []
    prior = _read_prior_silver_manifest(store, run_id)
    if prior:
        prior_sources = prior.get("sources") or {}
        for src, prev_count_raw in prior_sources.items():
            prev_count = int(prev_count_raw or 0)
            if prev_count <= 0:
                continue
            curr_count = int(source_counts.get(src, 0))
            drop = (prev_count - curr_count) / prev_count
            if drop <= 0:
                continue
            entry = {
                "fuente": src,
                "prev": prev_count,
                "curr": curr_count,
                "drop_ratio": round(drop, 4),
            }
            source_drops.append(entry)
            if drop >= warn_drop_ratio:
                warnings.append(
                    f"Caída en {src}: {prev_count} → {curr_count} ({_format_ratio(drop)})"
                )

    return _build_report(
        run_id,
        failures,
        warnings,
        total_rows=total_rows,
        unique_entity_id=unique_eids,
        duplicates=dupes,
        schema_version=schema_version,
        source_counts=source_counts,
        null_ratios=null_ratios,
        source_drops=source_drops,
        prior_run_id=(prior or {}).get("run_id"),
    )


def _build_report(
    run_id: str,
    failures: list[str],
    warnings: list[str],
    **extras: Any,
) -> dict[str, Any]:
    status = "FAIL" if failures else ("WARN" if warnings else "PASS")
    return {
        "layer": "silver",
        "run_id": run_id,
        "validated_at_utc": _utcnow_iso(),
        "status": status,
        "warnings": warnings,
        "failures": failures,
        **extras,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Valida la capa Silver del lake medallón")
    parser.add_argument("--run-id", default="latest", help="Run id (default: latest)")
    parser.add_argument(
        "--expected-sources",
        nargs="+",
        default=[],
        help="Fuentes esperadas (bnp mali joinnus places)",
    )
    parser.add_argument(
        "--max-null-ratio",
        type=float,
        default=0.30,
        help="Umbral de alerta para nulos en columnas críticas (0-1)",
    )
    parser.add_argument(
        "--warn-drop-ratio",
        type=float,
        default=0.70,
        help="Umbral de alerta para caída por fuente vs run previo (0-1)",
    )
    parser.add_argument(
        "--emit-json",
        action="store_true",
        help="Escribe reporte a logs/silver_quality_<run>.json",
    )
    args = parser.parse_args()

    store = get_store()
    run_id = _resolve_run_id(store, args.run_id)
    report = validate_silver(
        store=store,
        run_id=run_id,
        expected_sources=args.expected_sources or None,
        max_null_ratio=args.max_null_ratio,
        warn_drop_ratio=args.warn_drop_ratio,
    )

    print("[validate_silver] Resumen")
    print(f"  - run_id: {run_id}")
    print(f"  - status: {report['status']}")
    print(f"  - eventos totales: {report.get('total_rows')}")
    print(f"  - entity_id únicos: {report.get('unique_entity_id')}")
    print(f"  - duplicados: {report.get('duplicates')}")
    print(f"  - schema_version: {report.get('schema_version')}")
    if report.get("source_counts"):
        print(f"  - por fuente: {report['source_counts']}")
    if report.get("null_ratios"):
        print(
            "  - null ratios críticos: "
            f"{ {k: round(v, 4) for k, v in report['null_ratios'].items()} }"
        )
    drops = report.get("source_drops") or []
    if drops:
        print(f"  - caídas vs run previo ({report.get('prior_run_id')}):")
        for d in drops:
            print(
                f"    * {d['fuente']}: {d['prev']} → {d['curr']} "
                f"({_format_ratio(d['drop_ratio'])})"
            )
    if report["warnings"]:
        print("[validate_silver] WARNINGS")
        for w in report["warnings"]:
            print(f"  - {w}")
    if report["failures"]:
        print("[validate_silver] FAILURES")
        for f in report["failures"]:
            print(f"  - {f}")

    if args.emit_json:
        LOGS.mkdir(exist_ok=True)
        report_path = LOGS / f"silver_quality_{run_id}.json"
        latest_path = LOGS / "silver_quality_latest.json"
        for p in (report_path, latest_path):
            with open(p, "w", encoding="utf-8") as fh:
                json.dump(report, fh, ensure_ascii=False, indent=2)
        print(f"[validate_silver] Reporte JSON: {report_path}")

    sys.exit(1 if report["status"] == "FAIL" else 0)


if __name__ == "__main__":
    main()
