"""
pipeline.py
===========
Orquestador del pipeline cultural de Lima — modelo medallón.

Stages:
    1. Bronze — scrapers → raw payload al lake
    2. Silver — normalizer + geocoding → EventoEstandar Parquet (+ legacy CSV/JSON)
    3. Gold   — enrichment + embeddings → catálogo y vectores Parquet (+ sinks Mongo legacy)

Uso:
  python pipeline.py                          # Stage all (bronze→silver→gold) — comportamiento default
  python pipeline.py --stage=bronze           # Solo scraping + Bronze
  python pipeline.py --stage=silver           # Solo silver (re-normalizar y republicar legacy)
  python pipeline.py --stage=gold             # Solo gold (re-embeddings + sinks)
  python pipeline.py --sources bnp joinnus    # Subset de fuentes (en stage all)
  python pipeline.py --dry-run                # Usa eventos_estandar.csv existente, salta scraping
  python pipeline.py --skip-embeddings        # Omite embeddings en Gold
  python pipeline.py --write-mongo-web --write-mongo-reco  # Sinks Mongo legacy
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

# Paths
ROOT = Path(__file__).parent.parent
OUTPUT_DIR = ROOT / "output"
LOGS_DIR = ROOT / "logs"
OUTPUT_DIR.mkdir(exist_ok=True)
LOGS_DIR.mkdir(exist_ok=True)

# Optional: load local env vars from cultural_pipeline/.env
try:
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=ROOT / ".env")
except Exception:
    pass

# Logging
RUN_ID = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOGS_DIR / f"run_{RUN_ID}.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# sys.path para imports flat (scrapers/ pipeline/ embeddings/)
# ---------------------------------------------------------------------------

sys.path.insert(0, str(ROOT / "scrapers"))
sys.path.insert(0, str(ROOT / "pipeline"))
sys.path.insert(0, str(ROOT / "embeddings"))


from stages import bronze as bronze_stage  # noqa: E402
from stages import silver as silver_stage  # noqa: E402
from stages import gold as gold_stage  # noqa: E402
from stages.gold import GoldOptions  # noqa: E402
from storage import get_store  # noqa: E402


ALL_SOURCES: tuple[str, ...] = ("bnp", "mali", "joinnus", "places")


def _bool_env(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    return raw.strip().lower() in ("1", "true", "yes", "y", "on")


def _int_env(name: str, default: int, minimum: int = 1) -> int:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return max(minimum, default)
    try:
        value = int(raw)
    except Exception:
        return max(minimum, default)
    return max(minimum, value)


def _load_silver_from_legacy() -> pd.DataFrame:
    """Para `--dry-run` o `--stage=gold` standalone: lee el CSV vigente."""
    existing = OUTPUT_DIR / "eventos_estandar.csv"
    if not existing.exists():
        raise FileNotFoundError(
            f"No existe {existing}. Ejecuta el pipeline completo primero."
        )
    df = pd.read_csv(existing)
    log.info("📥 Cargados %s eventos de %s", len(df), existing)
    return df


def main(
    stage: str = "all",
    sources: list[str] | None = None,
    dry_run: bool = False,
    skip_embeddings: bool = False,
    enrich_deepseek: bool = False,
    write_mongo_web: bool = False,
    write_mongo_reco: bool = False,
) -> pd.DataFrame | None:
    log.info("🚀 Pipeline iniciado — run_id=%s stage=%s", RUN_ID, stage)

    sources = list(sources) if sources else list(ALL_SOURCES)
    is_full_run = (not dry_run) and set(sources) == set(ALL_SOURCES)
    store = get_store()

    df: pd.DataFrame | None = None
    raw_dfs: dict[str, pd.DataFrame] = {}

    # ---- Bronze ----------------------------------------------------------
    if stage in ("bronze", "all") and not dry_run:
        results = bronze_stage.run(
            run_id=RUN_ID,
            sources=sources,
            store=store,
            legacy_raw_dir=OUTPUT_DIR / "raw",
        )
        raw_dfs = {name: r.df for name, r in results.items()}
        if stage == "bronze":
            log.info("✅ Bronze stage completado")
            return None

    # ---- Silver ----------------------------------------------------------
    if dry_run:
        df = _load_silver_from_legacy()
    elif stage in ("silver", "all"):
        if not raw_dfs:
            # `--stage=silver` standalone: bronze ya fue ejecutado en otro run.
            # Para esta primera iteración usamos los raws legacy del filesystem
            # como fallback. La lectura desde Bronze events.jsonl queda para BD-9.
            raw_dir = OUTPUT_DIR / "raw"
            for src in sources:
                # tomar el CSV más reciente del source
                candidates = sorted(raw_dir.glob(f"{src}_*.csv"))
                if candidates:
                    raw_dfs[src] = pd.read_csv(candidates[-1])
            if not raw_dfs:
                log.warning(
                    "Silver standalone: no hay raws en %s. "
                    "Corre `--stage=bronze` primero.", raw_dir
                )
                return None

        df = silver_stage.run(
            run_id=RUN_ID,
            raw_dfs=raw_dfs,
            store=store,
            output_dir=OUTPUT_DIR,
            logs_dir=LOGS_DIR,
        )
        if df.empty:
            log.warning("⚠️ Silver vacío — abortando Gold")
            return df

        if stage == "silver":
            log.info("✅ Silver stage completado — %s eventos", len(df))
            return df

    # ---- Gold ------------------------------------------------------------
    if stage in ("gold", "all"):
        if df is None:
            df = _load_silver_from_legacy()

        opts = GoldOptions(
            skip_embeddings=skip_embeddings,
            enrich_deepseek=enrich_deepseek,
            write_mongo_web=write_mongo_web,
            write_mongo_reco=write_mongo_reco,
            hard_delete_stale=_bool_env("MONGO_HARD_DELETE_STALE", default=True),
            delete_after_missed_full_runs=_int_env(
                "MONGO_DELETE_AFTER_MISSED_FULL_RUNS", default=2, minimum=1
            ),
            is_full_run=is_full_run,
        )
        gold_stage.run(
            run_id=RUN_ID,
            df=df,
            store=store,
            opts=opts,
            output_dir=OUTPUT_DIR,
        )

    log.info("✅ Pipeline completado — %s eventos procesados", len(df) if df is not None else 0)
    return df


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pipeline de eventos culturales Lima (medallón)")
    parser.add_argument(
        "--stage",
        choices=["bronze", "silver", "gold", "all"],
        default="all",
        help="Stage a ejecutar (default: all)",
    )
    parser.add_argument(
        "--sources",
        nargs="+",
        choices=list(ALL_SOURCES),
        help="Fuentes a scrapear (default: todas)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="No hacer scraping, usar eventos_estandar.csv existente",
    )
    parser.add_argument(
        "--skip-embeddings",
        action="store_true",
        help="Omitir generación de embeddings en Gold",
    )
    parser.add_argument(
        "--enrich-deepseek",
        action="store_true",
        help="Enriquecer texto_embedding con DeepSeek antes de embeddings",
    )
    parser.add_argument(
        "--write-mongo-web",
        action="store_true",
        help="Publicar eventos a MongoDB (web) — sink legacy",
    )
    parser.add_argument(
        "--write-mongo-reco",
        action="store_true",
        help="Publicar embeddings a MongoDB (reco) — sink legacy",
    )
    args = parser.parse_args()

    main(
        stage=args.stage,
        sources=args.sources,
        dry_run=args.dry_run,
        skip_embeddings=args.skip_embeddings,
        enrich_deepseek=args.enrich_deepseek,
        write_mongo_web=args.write_mongo_web,
        write_mongo_reco=args.write_mongo_reco,
    )
