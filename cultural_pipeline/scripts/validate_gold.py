"""
Valida la capa Gold de un run (catálogo + vectores).

Lee:
  - gold/vectors/run_id=<id>/vectors.parquet  (embeddings + metadata mínima)
  - gold/_manifest_<id>.json                  (embedding_dim, model_name, counts)

Verifica:
  - shape de embeddings y consistencia con embedding_dim del manifest
  - valores finitos (no NaN/Inf)
  - tasa de norma unitaria (L2 ≈ 1) bajo `--norm-tolerance`
  - near-duplicates por similaridad coseno (`--dup-threshold`)
  - opcional: self-retrieval recall@k re-encodeando los `texto_embedding`
    del propio Parquet — equivalente a la métrica del legacy
    validate_embeddings.py pero leyendo del lake.

Cosmos vector index check queda señalado como `cosmos_check.skipped=True`
hasta que cierren BD-7 / BD-8.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np

ROOT = Path(__file__).resolve().parent.parent
LOGS = ROOT / "logs"

sys.path.insert(0, str(ROOT / "pipeline"))
sys.path.insert(0, str(ROOT / "scrapers"))

from contracts import (  # type: ignore[import-not-found]
    EMBEDDING_DIM,
    GOLD_SCHEMA_VERSION,
    gold_vectors_path,
    manifest_path,
)
from storage import get_store  # type: ignore[import-not-found]


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)


def _resolve_run_id(store, run_id: str) -> str:
    if run_id != "latest":
        return run_id
    latest = store.get_latest_run_id("gold")
    if not latest:
        raise FileNotFoundError(
            "Capa Gold sin _latest.json — corre `--stage=gold` o `--stage=all` primero"
        )
    return latest


def _self_retrieval(
    vectors: np.ndarray,
    texts: list[str],
    titles: list[str],
    model_name: str,
    top_k: int,
    sample_size: int,
) -> dict[str, Any]:
    try:
        from sentence_transformers import SentenceTransformer
    except ImportError:
        return {"available": False, "reason": "sentence-transformers no disponible"}

    if vectors.ndim != 2 or len(vectors) == 0:
        return {"available": False, "reason": "vectores con forma inválida"}

    usable = [i for i, t in enumerate(titles) if t and str(t).strip()]
    if not usable:
        return {"available": False, "reason": "sin títulos para consulta"}

    if sample_size > 0 and len(usable) > sample_size:
        rng = np.random.default_rng(42)
        usable = sorted(rng.choice(usable, size=sample_size, replace=False).tolist())

    queries = [titles[i] for i in usable]
    model = SentenceTransformer(model_name)
    q_vecs = model.encode(
        queries,
        batch_size=64,
        show_progress_bar=False,
        normalize_embeddings=True,
    )

    sims = np.asarray(q_vecs, dtype=np.float32) @ np.asarray(vectors, dtype=np.float32).T
    order = np.argsort(sims, axis=1)[:, ::-1]

    hits1 = 0
    hits_k = 0
    for row, target in enumerate(usable):
        ranked = order[row]
        if len(ranked) and int(ranked[0]) == target:
            hits1 += 1
        if target in ranked[:top_k]:
            hits_k += 1

    total = len(usable)
    return {
        "available": True,
        "sample_size": total,
        "top_k": top_k,
        "recall_at_1": hits1 / total if total else None,
        "recall_at_k": hits_k / total if total else None,
        "model": model_name,
    }


def validate_gold(
    store,
    run_id: str,
    top_k: int,
    sample_size: int,
    dup_threshold: float,
    norm_tolerance: float,
    skip_self_retrieval: bool,
) -> dict[str, Any]:
    failures: list[str] = []
    warnings: list[str] = []

    vectors_p = gold_vectors_path(run_id)
    manifest_p = manifest_path("gold", run_id)

    if not store.exists(manifest_p):
        warnings.append(f"GoldManifest no existe: {manifest_p}")
        manifest: dict[str, Any] = {}
    else:
        manifest = json.loads(store.read_bytes(manifest_p).decode("utf-8"))

    # Si el manifest indica que el run corrió sin embeddings, no es fallo
    # — solo validamos lo que haya (catálogo + manifest).
    manifest_vector_count = manifest.get("vector_count")
    if not store.exists(vectors_p):
        if manifest_vector_count == 0:
            warnings.append(
                "Gold vectores ausente — el run se ejecutó con --skip-embeddings "
                "(vector_count=0 en manifest). Validación de vectores omitida."
            )
            return _build_report(
                run_id,
                manifest,
                failures,
                warnings,
                n_vectors=0,
                vectors_skipped=True,
            )
        failures.append(f"Gold vectores no existe: {vectors_p}")
        return _build_report(run_id, manifest, failures, warnings)

    table = store.read_parquet(vectors_p)
    n_vectors = table.num_rows
    if n_vectors == 0:
        failures.append("Gold vectores vacío")
        return _build_report(run_id, manifest, failures, warnings, n_vectors=0)

    # Schema version del Parquet
    schema_meta = dict(table.schema.metadata or {})
    sv = schema_meta.get(b"schema_version")
    parquet_schema_version = sv.decode("utf-8") if isinstance(sv, bytes) else None
    if parquet_schema_version != GOLD_SCHEMA_VERSION:
        warnings.append(
            f"schema_version={parquet_schema_version!r} != contrato {GOLD_SCHEMA_VERSION!r}"
        )

    # Embedding column → ndarray
    emb_lists = table.column("embedding").to_pylist()
    vectors = np.asarray(emb_lists, dtype=np.float32)
    dim = int(vectors.shape[1]) if vectors.ndim == 2 else None

    if dim != EMBEDDING_DIM:
        failures.append(f"Dim observada={dim} != contrato {EMBEDDING_DIM}")

    manifest_dim = manifest.get("embedding_dim")
    if manifest_dim is not None and dim is not None and int(manifest_dim) != dim:
        warnings.append(
            f"embedding_dim manifest={manifest_dim} no coincide con Parquet dim={dim}"
        )

    # Finitud
    finite_ratio = float(np.isfinite(vectors).mean()) if vectors.size else 0.0
    if finite_ratio < 1.0:
        failures.append(f"Vectores con NaN/Inf — finite_ratio={finite_ratio:.4f}")

    # Norms
    norms = np.linalg.norm(vectors, axis=1) if vectors.ndim == 2 else np.array([])
    unit_mask = np.abs(norms - 1.0) <= norm_tolerance if norms.size else np.array([])
    unit_rate = float(unit_mask.mean()) if unit_mask.size else 0.0
    if unit_rate < 0.95:
        warnings.append(
            f"Tasa de norma unitaria={unit_rate:.4f} < 0.95 (tol={norm_tolerance})"
        )

    # Near-duplicates
    near_dup_pairs = 0
    near_dup_ratio: float | None = None
    similarity_stats: dict[str, Any] = {
        "top1_mean": None,
        "top1_median": None,
        "top1_p95": None,
    }
    if vectors.ndim == 2 and n_vectors > 1:
        sim = vectors @ vectors.T
        np.fill_diagonal(sim, -np.inf)
        top1 = sim.max(axis=1)
        similarity_stats = {
            "top1_mean": _safe_float(np.mean(top1)),
            "top1_median": _safe_float(np.median(top1)),
            "top1_p95": _safe_float(np.percentile(top1, 95)),
        }
        upper = np.triu(sim, k=1)
        near_dup_pairs = int(np.sum(upper >= dup_threshold))
        total_pairs = (n_vectors * (n_vectors - 1)) // 2
        near_dup_ratio = (near_dup_pairs / total_pairs) if total_pairs else 0.0

    # Self-retrieval (opcional)
    titles = table.column("titulo").to_pylist() if "titulo" in table.column_names else []
    texts = table.column("texto_embedding").to_pylist() if "texto_embedding" in table.column_names else []
    model_name = str(manifest.get("model_name") or "paraphrase-multilingual-MiniLM-L12-v2")

    if skip_self_retrieval:
        retrieval: dict[str, Any] = {"available": False, "reason": "skipped via --skip-self-retrieval"}
    else:
        retrieval = _self_retrieval(
            vectors=vectors,
            texts=texts,
            titles=titles,
            model_name=model_name,
            top_k=top_k,
            sample_size=sample_size,
        )

    if (
        retrieval.get("available")
        and retrieval.get("recall_at_k") is not None
        and float(retrieval["recall_at_k"]) < 0.90
    ):
        warnings.append(
            f"self_recall@{retrieval.get('top_k')}={retrieval['recall_at_k']:.3f} < 0.90"
        )

    return _build_report(
        run_id,
        manifest,
        failures,
        warnings,
        n_vectors=n_vectors,
        dim=dim,
        parquet_schema_version=parquet_schema_version,
        finite_ratio=finite_ratio,
        norm_unit_rate=unit_rate,
        norm_stats={
            "mean": _safe_float(np.mean(norms)) if norms.size else None,
            "min": _safe_float(np.min(norms)) if norms.size else None,
            "max": _safe_float(np.max(norms)) if norms.size else None,
            "tolerance": norm_tolerance,
        },
        similarity=similarity_stats,
        near_duplicates={
            "pairs": near_dup_pairs,
            "ratio": near_dup_ratio,
            "threshold": dup_threshold,
        },
        self_retrieval=retrieval,
        cosmos_check={"skipped": True, "reason": "Cosmos vCore wiring pendiente (BD-7/BD-8)"},
    )


def _build_report(
    run_id: str,
    manifest: dict[str, Any],
    failures: list[str],
    warnings: list[str],
    **extras: Any,
) -> dict[str, Any]:
    status = "FAIL" if failures else ("WARN" if warnings else "PASS")
    return {
        "layer": "gold",
        "run_id": run_id,
        "validated_at_utc": _utcnow_iso(),
        "status": status,
        "manifest": manifest,
        "warnings": warnings,
        "failures": failures,
        **extras,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Valida la capa Gold del lake medallón")
    parser.add_argument("--run-id", default="latest", help="Run id (default: latest)")
    parser.add_argument("--top-k", type=int, default=5, help="k para recall@k")
    parser.add_argument(
        "--sample-size",
        type=int,
        default=300,
        help="muestras para self-retrieval (0 = todo)",
    )
    parser.add_argument(
        "--dup-threshold",
        type=float,
        default=0.95,
        help="umbral coseno para near-duplicates",
    )
    parser.add_argument(
        "--norm-tolerance",
        type=float,
        default=1e-3,
        help="tolerancia para considerar ||v|| ≈ 1",
    )
    parser.add_argument(
        "--skip-self-retrieval",
        action="store_true",
        help="omite la métrica recall@k (más rápido / sin red)",
    )
    parser.add_argument(
        "--emit-json",
        action="store_true",
        help="Escribe reporte a logs/gold_quality_<run>.json",
    )
    args = parser.parse_args()

    store = get_store()
    run_id = _resolve_run_id(store, args.run_id)
    report = validate_gold(
        store=store,
        run_id=run_id,
        top_k=args.top_k,
        sample_size=args.sample_size,
        dup_threshold=args.dup_threshold,
        norm_tolerance=args.norm_tolerance,
        skip_self_retrieval=args.skip_self_retrieval,
    )

    print("[validate_gold] Resumen")
    print(f"  - run_id: {run_id}")
    print(f"  - status: {report['status']}")
    print(f"  - vectores: {report.get('n_vectors')} x {report.get('dim')}")
    print(f"  - schema_version: {report.get('parquet_schema_version')}")
    print(f"  - finite_ratio: {report.get('finite_ratio')}")
    print(f"  - norm.unit_rate: {report.get('norm_unit_rate')}")
    nd = report.get("near_duplicates") or {}
    print(f"  - near_duplicates(>= {nd.get('threshold')}): {nd.get('pairs')}")
    retr = report.get("self_retrieval") or {}
    if retr.get("available"):
        print(f"  - self_recall@1: {retr.get('recall_at_1')}")
        print(f"  - self_recall@{retr.get('top_k')}: {retr.get('recall_at_k')}")
    else:
        print(f"  - self_retrieval: skipped ({retr.get('reason')})")
    if report["warnings"]:
        print("[validate_gold] WARNINGS")
        for w in report["warnings"]:
            print(f"  - {w}")
    if report["failures"]:
        print("[validate_gold] FAILURES")
        for f in report["failures"]:
            print(f"  - {f}")

    if args.emit_json:
        LOGS.mkdir(exist_ok=True)
        report_path = LOGS / f"gold_quality_{run_id}.json"
        latest_path = LOGS / "gold_quality_latest.json"
        for p in (report_path, latest_path):
            with open(p, "w", encoding="utf-8") as fh:
                json.dump(report, fh, ensure_ascii=False, indent=2)
        print(f"[validate_gold] Reporte JSON: {report_path}")

    sys.exit(1 if report["status"] == "FAIL" else 0)


if __name__ == "__main__":
    main()
