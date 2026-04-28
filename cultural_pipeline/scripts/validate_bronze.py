"""
Valida la capa Bronze de un run.

Verifica:
  - existencia del manifest agregado bronze/_manifest_<run_id>.json
  - presencia de cada source listada en el manifest (events.jsonl + _source_manifest.json)
  - parseabilidad de events.jsonl (líneas válidas)
  - subset de fuentes esperado vs observado
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
LOGS = ROOT / "logs"

# Bootstrap sys.path para imports flat (igual convención que pipeline.py)
sys.path.insert(0, str(ROOT / "pipeline"))
sys.path.insert(0, str(ROOT / "scrapers"))

from contracts import bronze_path, manifest_path  # type: ignore[import-not-found]
from storage import get_store  # type: ignore[import-not-found]


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _resolve_run_id(store, run_id: str) -> str:
    if run_id != "latest":
        return run_id
    latest = store.get_latest_run_id("bronze")
    if not latest:
        raise FileNotFoundError(
            "Capa Bronze sin _latest.json — corre `--stage=bronze` primero"
        )
    return latest


def _count_jsonl_lines(raw: bytes) -> tuple[int, int]:
    """Devuelve (líneas_total, líneas_parseables_como_json)."""
    text = raw.decode("utf-8", errors="replace")
    lines = [line for line in text.splitlines() if line.strip()]
    parseable = 0
    for line in lines:
        try:
            json.loads(line)
            parseable += 1
        except Exception:
            pass
    return len(lines), parseable


def validate_bronze(
    store,
    run_id: str,
    expected_sources: list[str] | None = None,
) -> dict[str, Any]:
    failures: list[str] = []
    warnings: list[str] = []

    manifest_p = manifest_path("bronze", run_id)
    if not store.exists(manifest_p):
        failures.append(f"Manifest agregado no existe: {manifest_p}")
        return _build_report(run_id, failures, warnings, sources_report=[])

    manifest = json.loads(store.read_bytes(manifest_p).decode("utf-8"))
    sources_in_manifest = manifest.get("sources", [])

    sources_report: list[dict[str, Any]] = []
    for entry in sources_in_manifest:
        name = entry.get("name") or "?"
        payload_path = entry.get("payload_path") or bronze_path(
            name, run_id, "events.jsonl"
        )
        source_manifest_p = bronze_path(name, run_id, "_source_manifest.json")

        info: dict[str, Any] = {
            "source": name,
            "payload_path": payload_path,
            "scraper_version": entry.get("scraper_version"),
            "row_count_estimate": entry.get("row_count_estimate"),
        }

        if not store.exists(payload_path):
            failures.append(f"[{name}] payload ausente: {payload_path}")
            info["payload_present"] = False
            sources_report.append(info)
            continue
        info["payload_present"] = True

        if not store.exists(source_manifest_p):
            warnings.append(f"[{name}] _source_manifest.json ausente")
        info["source_manifest_present"] = store.exists(source_manifest_p)

        raw = store.read_bytes(payload_path)
        total, parseable = _count_jsonl_lines(raw)
        info["jsonl_lines"] = total
        info["jsonl_parseable"] = parseable
        if total > 0 and parseable != total:
            warnings.append(
                f"[{name}] {total - parseable} líneas no parseables en events.jsonl"
            )

        expected = entry.get("row_count_estimate")
        if expected is not None and total != expected:
            warnings.append(
                f"[{name}] row_count_estimate={expected} no coincide con líneas={total}"
            )

        sources_report.append(info)

    if expected_sources:
        observed = {s["source"] for s in sources_report}
        missing = [s for s in expected_sources if s not in observed]
        if missing:
            warnings.append(f"Fuentes esperadas no presentes: {missing}")

    return _build_report(run_id, failures, warnings, sources_report)


def _build_report(
    run_id: str,
    failures: list[str],
    warnings: list[str],
    sources_report: list[dict[str, Any]],
) -> dict[str, Any]:
    status = "FAIL" if failures else ("WARN" if warnings else "PASS")
    return {
        "layer": "bronze",
        "run_id": run_id,
        "validated_at_utc": _utcnow_iso(),
        "status": status,
        "sources": sources_report,
        "warnings": warnings,
        "failures": failures,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Valida la capa Bronze del lake medallón")
    parser.add_argument("--run-id", default="latest", help="Run id (default: latest)")
    parser.add_argument(
        "--expected-sources",
        nargs="+",
        default=[],
        help="Fuentes esperadas (ej: bnp mali joinnus places)",
    )
    parser.add_argument(
        "--emit-json",
        action="store_true",
        help="Escribe reporte a logs/bronze_quality_<run>.json",
    )
    args = parser.parse_args()

    store = get_store()
    run_id = _resolve_run_id(store, args.run_id)
    report = validate_bronze(
        store=store,
        run_id=run_id,
        expected_sources=args.expected_sources or None,
    )

    print("[validate_bronze] Resumen")
    print(f"  - run_id: {run_id}")
    print(f"  - status: {report['status']}")
    print(f"  - sources: {len(report['sources'])}")
    for s in report["sources"]:
        print(
            f"    * {s['source']}: payload={'✓' if s.get('payload_present') else '✗'} "
            f"jsonl_lines={s.get('jsonl_lines')} parseable={s.get('jsonl_parseable')}"
        )
    if report["warnings"]:
        print("[validate_bronze] WARNINGS")
        for w in report["warnings"]:
            print(f"  - {w}")
    if report["failures"]:
        print("[validate_bronze] FAILURES")
        for f in report["failures"]:
            print(f"  - {f}")

    if args.emit_json:
        LOGS.mkdir(exist_ok=True)
        report_path = LOGS / f"bronze_quality_{run_id}.json"
        latest_path = LOGS / "bronze_quality_latest.json"
        for p in (report_path, latest_path):
            with open(p, "w", encoding="utf-8") as fh:
                json.dump(report, fh, ensure_ascii=False, indent=2)
        print(f"[validate_bronze] Reporte JSON: {report_path}")

    sys.exit(1 if report["status"] == "FAIL" else 0)


if __name__ == "__main__":
    main()
