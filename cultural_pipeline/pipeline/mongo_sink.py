import json
import os
from datetime import datetime
from typing import Any, Optional

import pandas as pd


def _utcnow() -> datetime:
    return datetime.utcnow()


def _is_nan(value: Any) -> bool:
    try:
        return pd.isna(value)
    except Exception:
        return False


def _json_safe(value: Any) -> Any:
    # Check for containers first before trying _is_nan on arrays
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(v) for v in value]
    
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    
    if isinstance(value, datetime):
        return value.isoformat()
    
    # Now safe to check NaN (scalars only)
    if _is_nan(value):
        return None

    if isinstance(value, (str, int, float, bool)) or value is None:
        return value

    # numpy scalars, pandas types, etc.
    try:
        return _json_safe(value.item())
    except Exception:
        return str(value)


def _normalize_tags(tags: Any) -> list[str]:
    # Handle numpy arrays and pandas Series directly
    try:
        # If it's a numpy array or pandas Series, convert to list first
        if hasattr(tags, 'tolist'):
            tags = tags.tolist()
    except Exception:
        pass
    
    # Now check for None/NaN
    if tags is None:
        return []
    
    try:
        if _is_nan(tags):
            return []
    except (ValueError, TypeError):
        # _is_nan fails on containers; that's ok
        pass

    if isinstance(tags, list):
        return [str(t).strip() for t in tags if str(t).strip()]

    if isinstance(tags, str):
        raw = tags.strip()
        if not raw:
            return []
        # Try JSON list
        if raw.startswith("[") and raw.endswith("]"):
            try:
                parsed = json.loads(raw)
                if isinstance(parsed, list):
                    return [str(t).strip() for t in parsed if str(t).strip()]
            except Exception:
                pass
        # Fallback: comma-separated
        return [t.strip() for t in raw.split(",") if t.strip()]

    return [str(tags).strip()] if str(tags).strip() else []


def df_to_event_docs(df: pd.DataFrame) -> list[dict]:
    records = df.to_dict(orient="records")
    docs: list[dict] = []

    for rec in records:
        rec = {k: _json_safe(v) for k, v in rec.items()}

        entity_id = str(rec.get("entity_id") or rec.get("event_id") or "").strip()
        if not entity_id:
            continue

        rec.setdefault("entity_id", entity_id)
        rec.setdefault("event_id", entity_id)
        rec.setdefault("entity_type", "event")
        if not rec.get("url_origen") and rec.get("url_evento"):
            rec["url_origen"] = rec.get("url_evento")

        # Normalize known fields
        if "tags" in rec:
            rec["tags"] = _normalize_tags(rec.get("tags"))

        docs.append(rec)

    return docs


def _get_web_env_config() -> tuple[str, str, str]:
    uri = os.getenv("MONGO_URI_WEB") or ""
    db = os.getenv("MONGO_DB_WEB") or "turislima"
    coll = os.getenv("MONGO_COLL_WEB") or "entities"
    return uri, db, coll


def _get_reco_env_config() -> tuple[str, str, str]:
    uri = os.getenv("MONGO_URI_RECO") or ""
    db = os.getenv("MONGO_DB_RECO") or "turislima"
    coll = os.getenv("MONGO_COLL_RECO") or "entities_vectors"
    return uri, db, coll


def get_collection(mongo_uri: str, db_name: str, collection_name: str):
    if not mongo_uri:
        raise RuntimeError("Mongo URI vacío/no configurado")

    from pymongo import MongoClient

    client = MongoClient(
        mongo_uri,
        serverSelectionTimeoutMS=10_000,
        connectTimeoutMS=10_000,
        socketTimeoutMS=30_000,
        appname="turislima_datamining",
    )

    # Fail fast if not reachable
    client.admin.command("ping")

    coll = client[db_name][collection_name]
    return client, coll


def upsert_events_web(df: pd.DataFrame, run_id: str) -> dict:
    mongo_uri, db_name, coll_name = _get_web_env_config()
    client, coll = get_collection(mongo_uri, db_name, coll_name)

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
            "$setOnInsert": {
                "first_seen_at": now.isoformat(),
            },
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
    """Upsert vectors into reco collection.

    `embeddings` must align with df rows, shape (N, D).
    """
    mongo_uri, db_name, coll_name = _get_reco_env_config()
    client, coll = get_collection(mongo_uri, db_name, coll_name)

    from pymongo import UpdateOne

    now = _utcnow()

    # Import contract metadata from embedder
    try:
        from embedder import EMBEDDING_MODEL_NAME, EMBEDDING_DIM
    except Exception:
        EMBEDDING_MODEL_NAME = os.getenv("EMBEDDING_MODEL_NAME") or "unknown"
        EMBEDDING_DIM = int(os.getenv("EMBEDDING_DIM") or "0")

    docs = df_to_event_docs(df)

    if embeddings is None:
        raise RuntimeError("Embeddings no provistos para upsert reco")

    if len(docs) != len(embeddings):
        raise RuntimeError(f"Embeddings y docs no alinean: docs={len(docs)} embeddings={len(embeddings)}")

    ops = []
    for doc, vec in zip(docs, embeddings):
        entity_id = str(doc.get("entity_id") or doc.get("event_id") or "").strip()
        if not entity_id:
            continue

        # Ensure list[float]
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
                # Filters (keep minimal + stable)
                "tipo": doc.get("tipo"),
                "fecha_inicio": doc.get("fecha_inicio"),
                "precio": doc.get("precio"),
                "fuente": doc.get("fuente"),
                "ciudad": doc.get("ciudad"),
                "imagen_url": doc.get("imagen_url"),
                "url_origen": doc.get("url_origen") or doc.get("url_evento"),
            },
            "$setOnInsert": {
                "first_seen_at": now.isoformat(),
            },
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


def mark_inactive_not_seen_web(run_id: str) -> int:
    mongo_uri, db_name, coll_name = _get_web_env_config()
    client, coll = get_collection(mongo_uri, db_name, coll_name)

    now = _utcnow().isoformat()
    try:
        res = coll.update_many(
            {"last_seen_run_id": {"$ne": run_id}, "is_active": True},
            {"$set": {"is_active": False, "inactive_at": now}},
        )
        return int(res.modified_count)
    finally:
        client.close()


def delete_not_seen_web(run_id: str, min_missed_full_runs: int = 2) -> dict:
    """Increment missing streak and hard-delete stale docs after threshold.

    Intended for full runs only (all sources).
    """
    mongo_uri, db_name, coll_name = _get_web_env_config()
    client, coll = get_collection(mongo_uri, db_name, coll_name)

    threshold = max(1, int(min_missed_full_runs))
    now = _utcnow().isoformat()
    try:
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
    finally:
        client.close()


def mark_inactive_not_seen_reco(run_id: str) -> int:
    mongo_uri, db_name, coll_name = _get_reco_env_config()
    client, coll = get_collection(mongo_uri, db_name, coll_name)

    now = _utcnow().isoformat()
    try:
        res = coll.update_many(
            {"last_seen_run_id": {"$ne": run_id}, "is_active": True},
            {"$set": {"is_active": False, "inactive_at": now}},
        )
        return int(res.modified_count)
    finally:
        client.close()


def delete_not_seen_reco(run_id: str, min_missed_full_runs: int = 2) -> dict:
    """Increment missing streak and hard-delete stale vector docs after threshold.

    Intended for full runs only (all sources).
    """
    mongo_uri, db_name, coll_name = _get_reco_env_config()
    client, coll = get_collection(mongo_uri, db_name, coll_name)

    threshold = max(1, int(min_missed_full_runs))
    now = _utcnow().isoformat()
    try:
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
    finally:
        client.close()
