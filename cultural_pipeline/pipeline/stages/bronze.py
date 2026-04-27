"""
Stage Bronze: ejecuta los scrapers y persiste el payload crudo al lake.

Salidas:
  - lake://bronze/source=<name>/run_id=<id>/events.jsonl
  - lake://bronze/source=<name>/run_id=<id>/_source_manifest.json
  - lake://bronze/_manifest_<id>.json (agregado del run)
  - lake://bronze/_latest.json (puntero al run vigente)
  - output/raw/<name>_<id>.csv  (legacy auditoría — preservado)

Devuelve un dict[source -> ScrapeResult] que silver consume en memoria
cuando se corre `--stage=all`.
"""

from __future__ import annotations

import logging
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Iterable

import pandas as pd

from contracts import BronzeManifest  # type: ignore[import-not-found]
from _bronze import ScrapeResult, dump_to_bronze  # type: ignore[import-not-found]


log = logging.getLogger(__name__)


ALL_SOURCES: tuple[str, ...] = ("bnp", "mali", "joinnus", "places")


def _run_one(name: str) -> tuple[str, ScrapeResult]:
    """Ejecuta `run_with_payload()` del scraper correspondiente."""
    try:
        if name == "bnp":
            import scraper_bnp  # type: ignore[import-not-found]
            return name, scraper_bnp.run_with_payload()
        if name == "mali":
            import scraper_mali  # type: ignore[import-not-found]
            return name, scraper_mali.run_with_payload()
        if name == "joinnus":
            import scraper_joinnus  # type: ignore[import-not-found]
            return name, scraper_joinnus.run_with_payload()
        if name == "places":
            import scraper_google_places  # type: ignore[import-not-found]
            return name, scraper_google_places.run_with_payload()
        raise ValueError(f"Unknown source: {name!r}")
    except Exception as exc:
        log.error("❌ Scraper %s falló: %s\n%s", name, exc, traceback.format_exc())
        return name, ScrapeResult(
            df=pd.DataFrame(),
            raw_records=[],
            metadata={"scraper_version": f"{name}/error", "notes": f"exception: {exc}"},
        )


def run(
    run_id: str,
    sources: Iterable[str],
    store,
    legacy_raw_dir: Path | None = None,
    max_workers: int = 2,
) -> dict[str, ScrapeResult]:
    """Ejecuta los scrapers en paralelo y vuelca a Bronze.

    Args:
        run_id: identificador del run (mismo para toda la cadena).
        sources: subset de ("bnp","mali","joinnus","places").
        store: instancia BaseLakeStore (LocalStore/AzureStore).
        legacy_raw_dir: si se provee, escribe también el CSV crudo
            por scraper (compatibilidad con el flujo previo).
        max_workers: paralelismo del ThreadPoolExecutor.
    """
    sources = list(sources)
    log.info("🥉 Bronze stage iniciado — run_id=%s sources=%s", run_id, sources)

    results: dict[str, ScrapeResult] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(_run_one, s): s for s in sources}
        for future in as_completed(futures):
            name, result = future.result()
            results[name] = result
            log.info("  ✓ %s: %s registros parseados", name, len(result.raw_records))

    # Persistencia al lake
    entries = []
    for source, result in results.items():
        entry = dump_to_bronze(store, source, run_id, result)
        entries.append(entry)
    manifest = BronzeManifest(run_id=run_id, sources=entries)
    store.write_manifest("bronze", run_id, manifest)
    log.info("📦 Bronze manifest escrito (%s sources)", len(entries))

    # Legacy: output/raw/<source>_<run_id>.csv (sigue alimentando validadores actuales)
    if legacy_raw_dir is not None:
        legacy_raw_dir.mkdir(parents=True, exist_ok=True)
        for source, result in results.items():
            if not result.df.empty:
                path = legacy_raw_dir / f"{source}_{run_id}.csv"
                result.df.to_csv(path, index=False, encoding="utf-8-sig")

    return results
