"""
cosmos_sink.py
==============
Sink hacia Cosmos DB for MongoDB vCore. Espejo funcional de
`mongo_sink.py` (mismo contrato público), pero:

  - Lee la connection string desde `COSMOS_URI` o, si no está, desde
    Key Vault (secret `cosmos-uri`) usando `DefaultAzureCredential`.
  - Usa nombres de DB/colección por defecto alineados con los que
    `scripts/azure_provision.sh` escribe en `.env.azure`
    (`catalog.eventos` y `reco.eventos_vectors`).
  - Provee `ensure_vector_index(coll, dim, similarity)` para crear el
    índice HNSW `cosmosSearch` (idempotente).

Reusa los helpers de `mongo_sink` (`df_to_event_docs`, `get_collection`,
`_utcnow`) para evitar duplicación de la lógica de upsert.

El selector se hace via env var `RECO_BACKEND={atlas,cosmos}` desde
`gold.py`. Hasta el cutover (BD-12) ambos coexisten.
"""

from __future__ import annotations

import logging
import os
from functools import lru_cache
from typing import Any

import pandas as pd

from mongo_sink import (  # type: ignore[import-not-found]
    _utcnow,
    df_to_event_docs,
    get_collection,
)


log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# URI resolution (env-first, Key Vault fallback)
# ---------------------------------------------------------------------------


@lru_cache(maxsize=1)
def _resolve_cosmos_uri() -> str:
    """Devuelve el URI de Cosmos vCore.

    Orden de búsqueda:
      1. env `COSMOS_URI` (set explícitamente)
      2. Key Vault: lee secret `cosmos-uri` desde `KEY_VAULT_URI` o
         `KEY_VAULT_NAME`, autenticándose con `DefaultAzureCredential`
         (Managed Identity en Container Apps; az login en local).
    """
    uri = (os.getenv("COSMOS_URI") or "").strip()
    if uri:
        return uri

    vault_uri = (os.getenv("KEY_VAULT_URI") or "").strip()
    if not vault_uri:
        vault_name = (os.getenv("KEY_VAULT_NAME") or "").strip()
        if vault_name:
            vault_uri = f"https://{vault_name}.vault.azure.net"
    if not vault_uri:
        raise RuntimeError(
            "Cosmos URI no resuelto: define COSMOS_URI o KEY_VAULT_URI/KEY_VAULT_NAME"
        )

    from azure.identity import DefaultAzureCredential  # type: ignore[import-not-found]
    from azure.keyvault.secrets import SecretClient  # type: ignore[import-not-found]

    secret_name = os.getenv("COSMOS_URI_SECRET_NAME") or "cosmos-uri"
    client = SecretClient(vault_url=vault_uri, credential=DefaultAzureCredential())
    secret = client.get_secret(secret_name)
    secret_value = (secret.value or "").strip()
    if not secret_value:
        raise RuntimeError(
            f"Cosmos URI no resuelto: el secret '{secret_name}' en Key Vault '{vault_uri}' "
            "está vacío o no tiene valor"
        )
    return secret_value


def _get_catalog_env_config() -> tuple[str, str, str]:
    uri = _resolve_cosmos_uri()
    db = os.getenv("COSMOS_DB_CATALOG") or os.getenv("COSMOS_DB_WEB") or "catalog"
    coll = os.getenv("COSMOS_COLL_CATALOG") or "eventos"
    return uri, db, coll


def _get_reco_env_config() -> tuple[str, str, str]:
    uri = _resolve_cosmos_uri()
    db = os.getenv("COSMOS_DB_RECO") or "reco"
    coll = os.getenv("COSMOS_COLL_RECO") or "eventos_vectors"
    return uri, db, coll


# ---------------------------------------------------------------------------
# Vector index helper (cosmosSearch HNSW)
# ---------------------------------------------------------------------------


def ensure_vector_index(
    coll,
    dim: int,
    similarity: str = "COS",
    name: str = "vec_idx",
    m: int = 16,
    ef_construction: int = 64,
) -> dict[str, Any]:
    """Crea el índice vectorial HNSW si aún no existe.

    Cosmos vCore implementa vector search via el operador especial
    `$search.cosmosSearch` y un índice creado con `createIndexes`.
    Idempotente: si ya existe un índice con el mismo nombre, no falla.
    """
    db = coll.database
    cmd = {
        "createIndexes": coll.name,
        "indexes": [
            {
                "name": name,
                "key": {"embedding": "cosmosSearch"},
                "cosmosSearchOptions": {
                    "kind": "vector-hnsw",
                    "m": m,
                    "efConstruction": ef_construction,
                    "similarity": similarity,
                    "dimensions": dim,
                },
            }
        ],
    }
    try:
        return db.command(cmd)
    except Exception as exc:
        msg = str(exc).lower()
        if "already exists" in msg or "indexalreadyexists" in msg:
            log.info("Vector index %s ya existe", name)
            return {"already_exists": True, "name": name}
        raise


# ---------------------------------------------------------------------------
# API espejo de mongo_sink (catálogo web)
# ---------------------------------------------------------------------------


def upsert_events_web(df: pd.DataFrame, run_id: str) -> dict:
    cosmos_uri, db_name, coll_name = _get_catalog_env_config()
    client, coll = get_collection(cosmos_uri, db_name, coll_name)

    from pymongo import UpdateOne

    now = _utcnow()
    docs = df_to_event_docs(df)

    ops = []
    for doc in docs:
        entity_id = str(doc.get("entity_id") or doc.get("event_id") or "").strip()
        if not entity_id:
            continue
        update = {
            "$set": {
                **doc,
                "last_seen_at": now.isoformat(),
                "last_seen_run_id": run_id,
                "is_active": True,
                "missing_full_runs": 0,
            },
            "$setOnInsert": {"first_seen_at": now.isoformat()},
        }
        ops.append(UpdateOne({"_id": entity_id}, update, upsert=True))

    try:
        if not ops:
            return {"matched": 0, "modified": 0, "upserted": 0}
        res = coll.bulk_write(ops, ordered=False)
        return {
            "matched": int(res.matched_count),
            "modified": int(res.modified_count),
            "upserted": int(len(res.upserted_ids or {})),
        }
    finally:
        client.close()


def upsert_events_reco(df: pd.DataFrame, embeddings, run_id: str) -> dict:
    cosmos_uri, db_name, coll_name = _get_reco_env_config()
    client, coll = get_collection(cosmos_uri, db_name, coll_name)

    from pymongo import UpdateOne

    now = _utcnow()

    try:
        from embedder import EMBEDDING_DIM, EMBEDDING_MODEL_NAME  # type: ignore[import-not-found]
    except Exception:
        EMBEDDING_MODEL_NAME = os.getenv("EMBEDDING_MODEL_NAME") or "unknown"
        EMBEDDING_DIM = int(os.getenv("EMBEDDING_DIM") or "0")

    docs = df_to_event_docs(df)
    if embeddings is None:
        raise RuntimeError("Embeddings no provistos para upsert reco")
    if len(docs) != len(embeddings):
        raise RuntimeError(
            f"Embeddings y docs no alinean: docs={len(docs)} embeddings={len(embeddings)}"
        )

    # Asegurar índice HNSW antes del primer upsert
    if EMBEDDING_DIM and EMBEDDING_DIM > 0:
        try:
            ensure_vector_index(coll, dim=int(EMBEDDING_DIM))
        except Exception as exc:
            log.warning("ensure_vector_index falló (no crítico): %s", exc)

    ops = []
    for doc, vec in zip(docs, embeddings):
        entity_id = str(doc.get("entity_id") or doc.get("event_id") or "").strip()
        if not entity_id:
            continue
        try:
            embedding_list = [float(x) for x in vec]
        except Exception:
            embedding_list = [float(x) for x in getattr(vec, "tolist")()]

        update = {
            "$set": {
                "entity_id": entity_id,
                "event_id": entity_id,
                "entity_type": doc.get("entity_type") or "event",
                "embedding": embedding_list,
                "embedding_model": EMBEDDING_MODEL_NAME,
                "embedding_dim": EMBEDDING_DIM,
                "embedding_similarity": "cosine",
                "embedding_generated_at": now.isoformat(),
                "run_id": run_id,
                "last_seen_at": now.isoformat(),
                "last_seen_run_id": run_id,
                "is_active": True,
                "missing_full_runs": 0,
                "tipo": doc.get("tipo"),
                "fecha_inicio": doc.get("fecha_inicio"),
                "precio": doc.get("precio"),
                "fuente": doc.get("fuente"),
                "ciudad": doc.get("ciudad"),
                "imagen_url": doc.get("imagen_url"),
                "url_origen": doc.get("url_origen") or doc.get("url_evento"),
            },
            "$setOnInsert": {"first_seen_at": now.isoformat()},
        }
        ops.append(UpdateOne({"_id": entity_id}, update, upsert=True))

    try:
        if not ops:
            return {"matched": 0, "modified": 0, "upserted": 0}
        res = coll.bulk_write(ops, ordered=False)
        return {
            "matched": int(res.matched_count),
            "modified": int(res.modified_count),
            "upserted": int(len(res.upserted_ids or {})),
        }
    finally:
        client.close()


# ---------------------------------------------------------------------------
# Stale cleanup (mismo patrón que mongo_sink)
# ---------------------------------------------------------------------------


def _stale_inactivate(coll, run_id: str) -> int:
    now = _utcnow().isoformat()
    res = coll.update_many(
        {"last_seen_run_id": {"$ne": run_id}, "is_active": True},
        {"$set": {"is_active": False, "inactive_at": now}},
    )
    return int(res.modified_count)


def _stale_delete(coll, run_id: str, threshold: int) -> dict:
    now = _utcnow().isoformat()
    inc = coll.update_many(
        {"last_seen_run_id": {"$ne": run_id}},
        {
            "$inc": {"missing_full_runs": 1},
            "$set": {"is_active": False, "inactive_at": now},
        },
    )
    deleted = coll.delete_many(
        {
            "last_seen_run_id": {"$ne": run_id},
            "missing_full_runs": {"$gte": threshold},
        }
    )
    return {
        "incremented": int(inc.modified_count),
        "deleted": int(deleted.deleted_count),
        "threshold": threshold,
    }


def mark_inactive_not_seen_web(run_id: str) -> int:
    cosmos_uri, db_name, coll_name = _get_catalog_env_config()
    client, coll = get_collection(cosmos_uri, db_name, coll_name)
    try:
        return _stale_inactivate(coll, run_id)
    finally:
        client.close()


def delete_not_seen_web(run_id: str, min_missed_full_runs: int = 2) -> dict:
    cosmos_uri, db_name, coll_name = _get_catalog_env_config()
    client, coll = get_collection(cosmos_uri, db_name, coll_name)
    threshold = max(1, int(min_missed_full_runs))
    try:
        return _stale_delete(coll, run_id, threshold)
    finally:
        client.close()


def mark_inactive_not_seen_reco(run_id: str) -> int:
    cosmos_uri, db_name, coll_name = _get_reco_env_config()
    client, coll = get_collection(cosmos_uri, db_name, coll_name)
    try:
        return _stale_inactivate(coll, run_id)
    finally:
        client.close()


def delete_not_seen_reco(run_id: str, min_missed_full_runs: int = 2) -> dict:
    cosmos_uri, db_name, coll_name = _get_reco_env_config()
    client, coll = get_collection(cosmos_uri, db_name, coll_name)
    threshold = max(1, int(min_missed_full_runs))
    try:
        return _stale_delete(coll, run_id, threshold)
    finally:
        client.close()


# ---------------------------------------------------------------------------
# Vector search (consumido por el recomendador)
# ---------------------------------------------------------------------------


def search_similar(
    query_embedding,
    top_k: int = 5,
    filter_query: dict[str, Any] | None = None,
    project: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Ejecuta una consulta `$search.cosmosSearch` contra la colección reco.

    Args:
        query_embedding: vector consulta (list[float] o np.ndarray) ya
            normalizado a la misma dim que el índice HNSW.
        top_k: número de resultados a recuperar.
        filter_query: filtros adicionales aplicados como `$match` después
            de la búsqueda vectorial (ej. `{"is_active": True}`).
        project: proyección custom; por defecto devuelve campos base
            + score con `{"$meta": "searchScore"}`.

    Devuelve la lista de documentos con un campo `score` adicional.
    """
    cosmos_uri, db_name, coll_name = _get_reco_env_config()
    client, coll = get_collection(cosmos_uri, db_name, coll_name)

    try:
        vector = list(query_embedding)
    except Exception:
        vector = list(getattr(query_embedding, "tolist")())

    pipeline: list[dict[str, Any]] = [
        {
            "$search": {
                "cosmosSearch": {
                    "vector": vector,
                    "path": "embedding",
                    "k": int(top_k),
                }
            }
        }
    ]
    if filter_query:
        pipeline.append({"$match": filter_query})
    pipeline.append(
        {
            "$project": project
            or {
                "_id": 0,
                "entity_id": 1,
                "titulo": 1,
                "tipo": 1,
                "fuente": 1,
                "fecha_inicio": 1,
                "url_origen": 1,
                "imagen_url": 1,
                "score": {"$meta": "searchScore"},
            }
        }
    )

    try:
        return list(coll.aggregate(pipeline))
    finally:
        client.close()
