"""
pipeline.py
===========
Orquestador principal del pipeline cultural de Lima.

Flujo:
    1. Scraping paralelo (BNP + MALI + Joinnus + Places)
  2. Normalización → esquema EventoEstandar
  3. Guardado CSV/JSON para la app
  4. Generación de embeddings + índice FAISS
  5. Log del run

Uso:
  python pipeline.py                  # Ejecuta el pipeline completo
  python pipeline.py --dry-run        # Solo muestra datos existentes sin scraping
  python pipeline.py --sources bnp    # Solo un scraper
"""

import argparse
import json
import logging
import os
import sys
import traceback
from datetime import datetime
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd

# Paths
ROOT = Path(__file__).parent.parent
OUTPUT_DIR = ROOT / "output"
LOGS_DIR = ROOT / "logs"
OUTPUT_DIR.mkdir(exist_ok=True)
LOGS_DIR.mkdir(exist_ok=True)

# Optional: load local env vars from cultural_pipeline/.env (CI uses env/GitHub Secrets)
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
    ]
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Import scrapers y normalizer (rutas relativas)
# ---------------------------------------------------------------------------

sys.path.insert(0, str(ROOT / "scrapers"))
sys.path.insert(0, str(ROOT / "pipeline"))
sys.path.insert(0, str(ROOT / "embeddings"))


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


def run_scraper(name: str) -> tuple[str, pd.DataFrame]:
    """Ejecuta un scraper y devuelve (nombre, DataFrame crudo)"""
    try:
        if name == "bnp":
            import scraper_bnp
            return "bnp", scraper_bnp.run()
        elif name == "mali":
            import scraper_mali
            return "mali", scraper_mali.run()
        elif name == "joinnus":
            import scraper_joinnus
            return "joinnus", scraper_joinnus.run()
        elif name == "places":
            import scraper_google_places
            return "places", scraper_google_places.run()
    except Exception as e:
        log.error(f"❌ Scraper {name} falló: {e}\n{traceback.format_exc()}")
        return name, pd.DataFrame()


def save_outputs(df: pd.DataFrame, run_id: str):
    """Guarda el CSV, JSON y el snapshot del run"""
    # CSV principal (para la app)
    csv_path = OUTPUT_DIR / "eventos_estandar.csv"
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    log.info(f"📄 CSV guardado: {csv_path}")

    # JSON (para la API / app móvil)
    json_path = OUTPUT_DIR / "eventos_estandar.json"
    # Las listas se serializan directamente
    records = df.to_dict(orient="records")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2, default=str)
    log.info(f"📄 JSON guardado: {json_path}")

    # Snapshot histórico del run
    snapshot_dir = OUTPUT_DIR / "snapshots"
    snapshot_dir.mkdir(exist_ok=True)
    df.to_csv(snapshot_dir / f"eventos_{run_id}.csv", index=False, encoding="utf-8-sig")
    log.info(f"📦 Snapshot guardado: snapshots/eventos_{run_id}.csv")

    # Stats del run
    stats = {
        "run_id": run_id,
        "timestamp_utc": datetime.utcnow().isoformat(),
        "total_eventos": len(df),
        "por_entity_type": df["entity_type"].value_counts().to_dict() if "entity_type" in df.columns else {},
        "por_fuente": df["fuente"].value_counts().to_dict() if "fuente" in df.columns else {},
        "por_tipo": df["tipo"].value_counts().to_dict() if "tipo" in df.columns else {},
        "por_precio": df["precio"].value_counts().to_dict() if "precio" in df.columns else {},
    }
    with open(LOGS_DIR / f"stats_{run_id}.json", "w", encoding="utf-8") as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)
    log.info(f"📊 Stats: {stats}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(
    sources: list[str] = None,
    dry_run: bool = False,
    skip_embeddings: bool = False,
    enrich_deepseek: bool = False,
    write_mongo_web: bool = False,
    write_mongo_reco: bool = False,
):
    log.info(f"🚀 Pipeline iniciado — run_id={RUN_ID}")

    ALL_SOURCES = ["bnp", "mali", "joinnus", "places"]
    hard_delete_stale = _bool_env("MONGO_HARD_DELETE_STALE", default=True)
    delete_after_missed_full_runs = _int_env(
        "MONGO_DELETE_AFTER_MISSED_FULL_RUNS", default=2, minimum=1
    )
    if sources is None:
        sources = ALL_SOURCES

    raw_dfs = {}

    if dry_run:
        log.info("⚡ DRY-RUN — cargando datos existentes...")
        existing = OUTPUT_DIR / "eventos_estandar.csv"
        if existing.exists():
            df = pd.read_csv(existing)
            log.info(f"  {len(df)} eventos cargados de {existing}")
        else:
            log.warning("No hay datos previos. Ejecuta sin --dry-run primero.")
            return
    else:
        # Scraping en paralelo (max 2 workers para no sobrecargar)
        log.info(f"🕷️ Scraping fuentes: {sources}")
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = {pool.submit(run_scraper, s): s for s in sources}
            for future in as_completed(futures):
                name, df_raw = future.result()
                raw_dfs[name] = df_raw
                log.info(f"  ✓ {name}: {len(df_raw)} registros crudos")

        # Guardar raws para auditoría
        raw_dir = OUTPUT_DIR / "raw"
        raw_dir.mkdir(exist_ok=True)
        for name, df_raw in raw_dfs.items():
            if not df_raw.empty:
                df_raw.to_csv(raw_dir / f"{name}_{RUN_ID}.csv", index=False, encoding="utf-8-sig")

        # Normalización
        log.info("🔄 Normalizando datos...")
        from normalizer import normalize_all
        df = normalize_all(
            bnp_df=raw_dfs.get("bnp"),
            mali_df=raw_dfs.get("mali"),
            joinnus_df=raw_dfs.get("joinnus"),
            places_df=raw_dfs.get("places"),
        )

        if df.empty:
            log.warning("⚠️ No se obtuvieron eventos. Revisa los scrapers.")
            return

        # Geocoding — rellena lat/lng/geo_hash en eventos sin coordenadas
        if df["lat"].isna().any():
            try:
                from geocoder import geocode_events
                df = geocode_events(df, run_id=RUN_ID)
            except Exception as e:
                log.error(f"❌ Error en geocoding: {e}\n{traceback.format_exc()}")
                log.warning("Continuando sin geocoding.")

        # Guardar outputs
        save_outputs(df, RUN_ID)

    # Mongo sink (web) — after normalization / load
    if not df.empty and write_mongo_web:
        try:
            from mongo_sink import upsert_events_web, mark_inactive_not_seen_web, delete_not_seen_web

            log.info("🗄️ Publicando eventos a MongoDB (web)...")
            stats = upsert_events_web(df, run_id=RUN_ID)
            log.info(f"  ✓ Web upsert: {stats}")

            # Only inactivate on full, non-dry runs
            if (not dry_run) and set(sources) == set(ALL_SOURCES):
                if hard_delete_stale:
                    cleanup_stats = delete_not_seen_web(
                        run_id=RUN_ID,
                        min_missed_full_runs=delete_after_missed_full_runs,
                    )
                    log.info(
                        "  🧹 Web stale incrementados: %s | eliminados (>= %s faltas completas): %s",
                        cleanup_stats.get("incremented"),
                        cleanup_stats.get("threshold"),
                        cleanup_stats.get("deleted"),
                    )
                else:
                    inactivated = mark_inactive_not_seen_web(run_id=RUN_ID)
                    log.info(f"  ✓ Web inactivos marcados: {inactivated}")
            else:
                log.info("  ℹ️ Limpieza web omitida (run parcial o dry-run)")
        except Exception as e:
            log.error(f"❌ Error publicando a MongoDB (web): {e}\n{traceback.format_exc()}")
            log.warning("Continuando sin publicación web.")

    # Optional enrichment (DeepSeek)
    if not dry_run and not df.empty and enrich_deepseek:
        try:
            sys.path.insert(0, str(ROOT / "embeddings"))
            from enricher import enrich_event, append_enrichment_to_texto_embedding, max_events_to_enrich

            log.info("🧩 Enriqueciendo texto_embedding (DeepSeek)...")
            limit = max_events_to_enrich()
            enriched = 0
            new_texts = []
            for _, row in df.iterrows():
                if enriched >= limit:
                    new_texts.append(row.get("texto_embedding"))
                    continue

                event = {
                    "titulo": row.get("titulo"),
                    "descripcion": row.get("descripcion"),
                    "tipo": row.get("tipo"),
                    "lugar": row.get("lugar"),
                    "imagen_url": row.get("imagen_url"),
                }
                enrichment = enrich_event(event)
                new_texts.append(append_enrichment_to_texto_embedding(row.get("texto_embedding"), enrichment))
                enriched += 1

            df["texto_embedding"] = new_texts
            log.info(f"  ✓ Enrichment aplicado a {enriched} eventos (tope={limit})")
        except Exception as e:
            log.error(f"❌ Error en enrichment DeepSeek: {e}\n{traceback.format_exc()}")
            log.warning("Continuando sin enrichment.")

    # Embeddings
    if not skip_embeddings:
        log.info("🧠 Generando embeddings...")
        try:
            from embedder import generate_embeddings

            embeddings = generate_embeddings(df, run_id=RUN_ID)

            try:
                from catalog_exporter import export_poi_catalog
                export_poi_catalog(df, embeddings, OUTPUT_DIR)
            except Exception as e:
                log.error(f"❌ Error exportando poi_catalog: {e}\n{traceback.format_exc()}")
                log.warning("Continuando sin poi_catalog.json.")

            try:
                from build_faiss_index import build_index
                n_idx, _, _ = build_index(OUTPUT_DIR)
                log.info(f"🔍 FAISS index construido: {n_idx} vectores")
            except Exception as e:
                log.error(f"❌ Error construyendo FAISS index: {e}\n{traceback.format_exc()}")
                log.warning("Continuando sin faiss_index.bin.")

            if write_mongo_reco:
                from mongo_sink import upsert_events_reco, mark_inactive_not_seen_reco, delete_not_seen_reco

                log.info("🗄️ Publicando embeddings a MongoDB (reco)...")
                stats = upsert_events_reco(df, embeddings=embeddings, run_id=RUN_ID)
                log.info(f"  ✓ Reco upsert: {stats}")

                # Only inactivate on full, non-dry runs
                if (not dry_run) and set(sources) == set(ALL_SOURCES):
                    if hard_delete_stale:
                        cleanup_stats = delete_not_seen_reco(
                            run_id=RUN_ID,
                            min_missed_full_runs=delete_after_missed_full_runs,
                        )
                        log.info(
                            "  🧹 Reco stale incrementados: %s | eliminados (>= %s faltas completas): %s",
                            cleanup_stats.get("incremented"),
                            cleanup_stats.get("threshold"),
                            cleanup_stats.get("deleted"),
                        )
                    else:
                        inactivated = mark_inactive_not_seen_reco(run_id=RUN_ID)
                        log.info(f"  ✓ Reco inactivos marcados: {inactivated}")
                else:
                    log.info("  ℹ️ Limpieza reco omitida (run parcial o dry-run)")
        except Exception as e:
            log.error(f"❌ Error al generar embeddings: {e}\n{traceback.format_exc()}")
            log.warning("Pipeline completado SIN embeddings.")
    else:
        log.info("⏭️ Embeddings omitidos (--skip-embeddings)")

    log.info(f"✅ Pipeline completado — {len(df)} eventos procesados")
    return df


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pipeline de eventos culturales Lima")
    parser.add_argument("--sources", nargs="+", choices=["bnp", "mali", "joinnus", "places"],
                        help="Fuentes a scrapear (default: todas)")
    parser.add_argument("--dry-run", action="store_true",
                        help="No hacer scraping, usar datos existentes")
    parser.add_argument("--skip-embeddings", action="store_true",
                        help="Omitir generación de embeddings")
    parser.add_argument("--enrich-deepseek", action="store_true",
                        help="Enriquecer texto_embedding con DeepSeek (caption/tags/resumen)")
    parser.add_argument("--write-mongo-web", action="store_true",
                        help="Publicar eventos estandarizados a MongoDB (web) usando MONGO_URI_WEB")
    parser.add_argument("--write-mongo-reco", action="store_true",
                        help="Publicar embeddings a MongoDB (reco) usando MONGO_URI_RECO")
    args = parser.parse_args()

    main(
        sources=args.sources,
        dry_run=args.dry_run,
        skip_embeddings=args.skip_embeddings,
        enrich_deepseek=args.enrich_deepseek,
        write_mongo_web=args.write_mongo_web,
        write_mongo_reco=args.write_mongo_reco,
    )
