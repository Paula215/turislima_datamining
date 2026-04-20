from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parent.parent
OUTPUT = ROOT / "output"
LOGS = ROOT / "logs"


def _to_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def fail(msg: str) -> None:
    print(f"[validar_output] ERROR: {msg}")
    sys.exit(1)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Valida outputs normalizados del pipeline")
    parser.add_argument(
        "--expected-sources",
        nargs="+",
        default=[],
        help="Fuentes esperadas en el output de esta corrida (ej: bnp mali joinnus places)",
    )
    parser.add_argument(
        "--max-null-ratio",
        type=float,
        default=0.30,
        help="Umbral de alerta para nulos en columnas criticas (0-1)",
    )
    parser.add_argument(
        "--warn-drop-ratio",
        type=float,
        default=0.70,
        help="Umbral de alerta para caida de cobertura por fuente vs corrida anterior (0-1)",
    )
    parser.add_argument(
        "--emit-json",
        action="store_true",
        help="Genera reporte JSON en logs/output_quality_<run>.json y output_quality_latest.json",
    )
    return parser.parse_args()


def _latest_stats_pair() -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    stats_files = sorted(LOGS.glob("stats_*.json"))
    if not stats_files:
        return None, None

    current = stats_files[-1]
    previous = stats_files[-2] if len(stats_files) >= 2 else None

    with open(current, "r", encoding="utf-8") as fh:
        current_stats = json.load(fh)

    previous_stats = None
    if previous is not None:
        with open(previous, "r", encoding="utf-8") as fh:
            previous_stats = json.load(fh)
    return current_stats, previous_stats


def _format_ratio(value: float) -> str:
    return f"{(value * 100):.1f}%"


def _normalize_source(source: str) -> str:
    alias = {
        "places": "google_places",
    }
    key = str(source or "").strip().lower()
    return alias.get(key, key)


def main() -> None:
    args = _parse_args()
    csv_path = OUTPUT / "eventos_estandar.csv"
    json_path = OUTPUT / "eventos_estandar.json"

    if not csv_path.exists():
        fail(f"No existe {csv_path}")
    if not json_path.exists():
        fail(f"No existe {json_path}")

    df = pd.read_csv(csv_path)
    if df.empty:
        fail("El CSV estandar esta vacio")

    required_columns = [
        "entity_id",
        "entity_type",
        "titulo",
        "fuente",
        "texto_embedding",
        "url_origen",
    ]
    missing = [c for c in required_columns if c not in df.columns]
    if missing:
        fail(f"Faltan columnas requeridas en CSV: {missing}")

    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list) or not data:
        fail("El JSON estandar no contiene una lista de eventos valida")

    unique_event_ids = df["entity_id"].nunique(dropna=True)
    total_events = len(df)
    dupes = total_events - unique_event_ids

    warnings: list[str] = []
    failures: list[str] = []

    if dupes > 0:
        failures.append(f"Hay {dupes} entity_id duplicados")

    entity_counts = df["entity_type"].value_counts(dropna=False).to_dict()
    source_counts = df["fuente"].value_counts(dropna=False).to_dict()

    critical_null_columns = ["entity_id", "entity_type", "titulo", "fuente", "texto_embedding"]
    null_ratios: dict[str, float] = {}
    for col in critical_null_columns:
        null_ratio = float(df[col].isna().mean())
        null_ratios[col] = null_ratio
        if null_ratio > args.max_null_ratio:
            warnings.append(
                f"Nulos altos en {col}: {_format_ratio(null_ratio)} (umbral {_format_ratio(args.max_null_ratio)})"
            )

    if args.expected_sources:
        expected_sources = [_normalize_source(s) for s in args.expected_sources]
        missing_sources = [s for s in expected_sources if source_counts.get(s, 0) == 0]
        if missing_sources:
            warnings.append(f"Fuentes sin registros en esta corrida: {missing_sources}")

    current_stats, previous_stats = _latest_stats_pair()
    source_drops: list[dict[str, Any]] = []
    if previous_stats is not None:
        prev_sources = previous_stats.get("por_fuente") or {}
        for src, prev_count_raw in prev_sources.items():
            prev_count = _to_int(prev_count_raw)
            if prev_count <= 0:
                continue
            curr_count = _to_int(source_counts.get(src, 0))
            drop_ratio = (prev_count - curr_count) / prev_count
            if drop_ratio <= 0:
                continue
            source_drops.append({
                "fuente": src,
                "prev": prev_count,
                "curr": curr_count,
                "drop_ratio": round(drop_ratio, 4),
            })
            if drop_ratio >= args.warn_drop_ratio:
                warnings.append(
                    f"Caida de cobertura en {src}: {prev_count} -> {curr_count} ({_format_ratio(drop_ratio)})"
                )

    print("[validar_output] Resumen")
    print(f"  - eventos totales: {total_events}")
    print(f"  - entity_id unicos: {unique_event_ids}")
    print(f"  - posibles duplicados: {dupes}")
    print(f"  - por entity_type: {entity_counts}")
    print(f"  - por fuente: {source_counts}")
    print(f"  - null ratios criticos: { {k: round(v, 4) for k, v in null_ratios.items()} }")

    if source_drops:
        print("  - caidas vs corrida anterior:")
        for d in source_drops:
            print(
                "    * {fuente}: {prev} -> {curr} ({ratio})".format(
                    fuente=d["fuente"],
                    prev=d["prev"],
                    curr=d["curr"],
                    ratio=_format_ratio(d["drop_ratio"]),
                )
            )

    if warnings:
        print("[validar_output] WARNINGS")
        for w in warnings:
            print(f"  - {w}")

    if failures:
        print("[validar_output] FAILURES")
        for fmsg in failures:
            print(f"  - {fmsg}")

    report = {
        "generated_at_utc": datetime.utcnow().isoformat(),
        "status": "FAIL" if failures else ("WARN" if warnings else "PASS"),
        "summary": {
            "total_entities": total_events,
            "unique_entity_id": unique_event_ids,
            "duplicates": dupes,
            "entity_type_counts": entity_counts,
            "source_counts": source_counts,
            "null_ratios": {k: round(v, 6) for k, v in null_ratios.items()},
        },
        "comparison": {
            "current_run_id": (current_stats or {}).get("run_id"),
            "previous_run_id": (previous_stats or {}).get("run_id") if previous_stats else None,
            "source_drops": source_drops,
        },
        "warnings": warnings,
        "failures": failures,
    }

    if args.emit_json:
        LOGS.mkdir(exist_ok=True)
        run_id = (current_stats or {}).get("run_id") or datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        report_path = LOGS / f"output_quality_{run_id}.json"
        latest_path = LOGS / "output_quality_latest.json"
        with open(report_path, "w", encoding="utf-8") as fh:
            json.dump(report, fh, ensure_ascii=False, indent=2)
        with open(latest_path, "w", encoding="utf-8") as fh:
            json.dump(report, fh, ensure_ascii=False, indent=2)
        print(f"[validar_output] Reporte JSON: {report_path}")

    if failures:
        fail("Se detectaron fallas criticas en output estandar")

    print("[validar_output] OK")


if __name__ == "__main__":
    main()
