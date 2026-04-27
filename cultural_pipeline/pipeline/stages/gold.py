"""
Stage Gold: enriquecimiento opcional, embeddings, catálogo y vectores
listos-para-servir, además de los sinks legacy (Mongo web/reco, FAISS).

Salidas:
  - lake://gold/catalog_eventos/run_id=<id>/part-0000.parquet
  - lake://gold/vectors/run_id=<id>/vectors.parquet
  - lake://gold/_manifest_<id>.json
  - embeddings/vectors_<id>.npy + metadata_<id>.json + contract_<id>.json (legacy)
  - embeddings/faiss_<id>.index (legacy, opcional)
  - MongoDB Atlas web + reco upserts (legacy, hasta cutover BD-7/BD-12)
"""

from __future__ import annotations

import logging
import os
import traceback
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa

from contracts import (  # type: ignore[import-not-found]
    EMBEDDING_DIM,
    GOLD_SCHEMA_VERSION,
    GoldManifest,
    gold_catalog_arrow_schema,
    gold_vectors_arrow_schema,
)


log = logging.getLogger(__name__)


@dataclass
class GoldOptions:
    skip_embeddings: bool = False
    enrich_deepseek: bool = False
    write_mongo_web: bool = False
    write_mongo_reco: bool = False
    hard_delete_stale: bool = True
    delete_after_missed_full_runs: int = 2
    is_full_run: bool = True  # solo limpiamos stale en runs completos


def _maybe_enrich_deepseek(df: pd.DataFrame) -> pd.DataFrame:
    try:
        from enricher import (  # type: ignore[import-not-found]
            append_enrichment_to_texto_embedding,
            enrich_event,
            max_events_to_enrich,
        )

        log.info("🧩 Enriqueciendo texto_embedding (DeepSeek)...")
        limit = max_events_to_enrich()
        enriched = 0
        new_texts: list = []
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
            new_texts.append(
                append_enrichment_to_texto_embedding(
                    row.get("texto_embedding"), enrichment
                )
            )
            enriched += 1
        df = df.copy()
        df["texto_embedding"] = new_texts
        log.info("  ✓ Enrichment aplicado a %s eventos (tope=%s)", enriched, limit)
    except Exception as e:
        log.error("❌ Error en enrichment DeepSeek: %s\n%s", e, traceback.format_exc())
        log.warning("Continuando sin enrichment.")
    return df


def _write_gold_catalog(df: pd.DataFrame, run_id: str, store) -> str:
    schema = gold_catalog_arrow_schema()
    aligned = {}
    for f in schema:
        col = f.name
        if col == "schema_version":
            aligned[col] = [GOLD_SCHEMA_VERSION] * len(df)
        elif col in df.columns:
            aligned[col] = df[col]
        else:
            aligned[col] = [None] * len(df)
    cat_df = pd.DataFrame(aligned)
    for date_col in ("fecha_inicio", "fecha_fin", "fecha_run"):
        if date_col in cat_df.columns:
            cat_df[date_col] = pd.to_datetime(cat_df[date_col], errors="coerce").dt.date
    table = pa.Table.from_pandas(cat_df, schema=schema, preserve_index=False)
    return store.write_gold_catalog(run_id, table)


def _write_gold_vectors(
    df: pd.DataFrame,
    embeddings: np.ndarray,
    run_id: str,
    store,
    model_name: str,
) -> str:
    if len(df) != len(embeddings):
        raise ValueError(
            f"Vectors/df length mismatch: {len(df)} vs {len(embeddings)}"
        )
    rows = []
    for i, (_, row) in enumerate(df.iterrows()):
        fecha_inicio_ts = pd.to_datetime(row.get("fecha_inicio"), errors="coerce")
        rows.append(
            {
                "entity_id": row.get("entity_id"),
                "titulo": row.get("titulo"),
                "tipo": row.get("tipo"),
                "fuente": row.get("fuente"),
                "fecha_inicio": (
                    None if pd.isna(fecha_inicio_ts) else fecha_inicio_ts.date()
                ),
                "texto_embedding": row.get("texto_embedding"),
                "embedding": embeddings[i].astype("float32").tolist(),
                "model_name": model_name,
                "schema_version": GOLD_SCHEMA_VERSION,
            }
        )
    table = pa.Table.from_pylist(rows, schema=gold_vectors_arrow_schema())
    return store.write_gold_vectors(run_id, table)


def _resolve_model_name() -> str:
    return os.getenv("EMBEDDING_MODEL_NAME", "paraphrase-multilingual-MiniLM-L12-v2")


def run(
    run_id: str,
    df: pd.DataFrame,
    store,
    opts: GoldOptions,
    output_dir: Path,
) -> None:
    """Pipeline Gold: enrichment + sinks Mongo + embeddings + Gold parquet."""
    log.info("🥇 Gold stage iniciado — run_id=%s", run_id)

    if df.empty:
        log.warning("Gold: df vacío, nada que procesar")
        return

    # Mongo web (legacy sink — corre con df pre-embeddings, igual que el flujo viejo)
    if opts.write_mongo_web:
        try:
            from mongo_sink import (  # type: ignore[import-not-found]
                delete_not_seen_web,
                mark_inactive_not_seen_web,
                upsert_events_web,
            )

            log.info("🗄️ Publicando eventos a MongoDB (web)...")
            stats = upsert_events_web(df, run_id=run_id)
            log.info("  ✓ Web upsert: %s", stats)
            if opts.is_full_run:
                if opts.hard_delete_stale:
                    cleanup = delete_not_seen_web(
                        run_id=run_id,
                        min_missed_full_runs=opts.delete_after_missed_full_runs,
                    )
                    log.info(
                        "  🧹 Web stale incrementados: %s | eliminados (>= %s faltas): %s",
                        cleanup.get("incremented"),
                        cleanup.get("threshold"),
                        cleanup.get("deleted"),
                    )
                else:
                    inactivated = mark_inactive_not_seen_web(run_id=run_id)
                    log.info("  ✓ Web inactivos marcados: %s", inactivated)
            else:
                log.info("  ℹ️ Limpieza web omitida (run parcial)")
        except Exception as e:
            log.error("❌ Error publicando a MongoDB (web): %s\n%s", e, traceback.format_exc())

    # Enrichment opcional ANTES de generar embeddings
    if opts.enrich_deepseek:
        df = _maybe_enrich_deepseek(df)

    # Embeddings + Gold artefactos
    embeddings = None
    if not opts.skip_embeddings:
        try:
            from embedder import generate_embeddings  # type: ignore[import-not-found]

            log.info("🧠 Generando embeddings...")
            embeddings = generate_embeddings(df, run_id=run_id)

            # Catálogo legacy (poi_catalog.json)
            try:
                from catalog_exporter import export_poi_catalog  # type: ignore[import-not-found]
                export_poi_catalog(df, embeddings, output_dir)
            except Exception as e:
                log.error("❌ Error exportando poi_catalog: %s\n%s", e, traceback.format_exc())

            # Índice FAISS legacy
            try:
                from build_faiss_index import build_index  # type: ignore[import-not-found]
                n_idx, _, _ = build_index(output_dir)
                log.info("🔍 FAISS index construido: %s vectores", n_idx)
            except Exception as e:
                log.error("❌ Error construyendo FAISS index: %s\n%s", e, traceback.format_exc())

            # Mongo reco (legacy)
            if opts.write_mongo_reco:
                try:
                    from mongo_sink import (  # type: ignore[import-not-found]
                        delete_not_seen_reco,
                        mark_inactive_not_seen_reco,
                        upsert_events_reco,
                    )

                    log.info("🗄️ Publicando embeddings a MongoDB (reco)...")
                    stats = upsert_events_reco(df, embeddings=embeddings, run_id=run_id)
                    log.info("  ✓ Reco upsert: %s", stats)
                    if opts.is_full_run:
                        if opts.hard_delete_stale:
                            cleanup = delete_not_seen_reco(
                                run_id=run_id,
                                min_missed_full_runs=opts.delete_after_missed_full_runs,
                            )
                            log.info(
                                "  🧹 Reco stale incrementados: %s | eliminados: %s",
                                cleanup.get("incremented"),
                                cleanup.get("deleted"),
                            )
                        else:
                            inactivated = mark_inactive_not_seen_reco(run_id=run_id)
                            log.info("  ✓ Reco inactivos marcados: %s", inactivated)
                    else:
                        log.info("  ℹ️ Limpieza reco omitida (run parcial)")
                except Exception as e:
                    log.error("❌ Error publicando a MongoDB (reco): %s\n%s", e, traceback.format_exc())
        except Exception as e:
            log.error("❌ Error al generar embeddings: %s\n%s", e, traceback.format_exc())
            log.warning("Pipeline Gold completado SIN embeddings.")
    else:
        log.info("⏭️ Embeddings omitidos (--skip-embeddings)")

    # Gold Parquet (catálogo + vectores) — siempre que haya datos
    catalog_path = _write_gold_catalog(df, run_id, store)
    log.info("📦 Gold catálogo: %s (%s filas)", catalog_path, len(df))

    vectors_path = ""
    vector_count = 0
    if embeddings is not None:
        vectors_path = _write_gold_vectors(
            df, embeddings, run_id, store, _resolve_model_name()
        )
        vector_count = int(len(embeddings))
        log.info("📦 Gold vectores: %s (%s vectores, dim=%s)",
                 vectors_path, vector_count, EMBEDDING_DIM)

    gold_manifest = GoldManifest(
        run_id=run_id,
        schema_version=GOLD_SCHEMA_VERSION,
        catalog_path=catalog_path,
        vectors_path=vectors_path,
        catalog_count=int(len(df)),
        vector_count=vector_count,
        embedding_dim=EMBEDDING_DIM,
        model_name=_resolve_model_name(),
    )
    store.write_manifest("gold", run_id, gold_manifest)
