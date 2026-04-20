from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np


ROOT = Path(__file__).resolve().parent.parent
EMBEDDINGS_DIR = ROOT / "embeddings"


@dataclass
class ValidationContext:
    run_id: str
    vectors_path: Path
    metadata_path: Path
    contract_path: Path


def _resolve_paths(run_id: str) -> ValidationContext:
    if run_id == "latest":
        vectors_path = EMBEDDINGS_DIR / "vectors_latest.npy"
        metadata_path = EMBEDDINGS_DIR / "metadata_latest.json"
        contract_path = EMBEDDINGS_DIR / "contract_latest.json"
    else:
        vectors_path = EMBEDDINGS_DIR / f"vectors_{run_id}.npy"
        metadata_path = EMBEDDINGS_DIR / f"metadata_{run_id}.json"
        contract_path = EMBEDDINGS_DIR / f"contract_{run_id}.json"

    if not vectors_path.exists():
        raise FileNotFoundError(f"No existe {vectors_path}")
    if not metadata_path.exists():
        raise FileNotFoundError(f"No existe {metadata_path}")

    if contract_path.exists():
        with open(contract_path, "r", encoding="utf-8") as f:
            contract = json.load(f)
        contract_run_id = contract.get("run_id")
        if contract_run_id:
            run_id = str(contract_run_id)

    return ValidationContext(
        run_id=run_id,
        vectors_path=vectors_path,
        metadata_path=metadata_path,
        contract_path=contract_path,
    )


def _load_json(path: Path) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _safe_float(value: float | np.floating | None) -> float | None:
    if value is None:
        return None
    return float(value)


def _self_retrieval_metrics(
    vectors: np.ndarray,
    metadata: list[dict[str, Any]],
    model_name: str,
    top_k: int,
    sample_size: int,
) -> dict[str, Any]:
    try:
        from sentence_transformers import SentenceTransformer
    except ImportError:
        return {
            "available": False,
            "reason": "sentence-transformers no disponible",
        }

    if not metadata:
        return {"available": False, "reason": "metadata vacia"}

    def _clean(value: Any) -> str:
        text = str(value or "").strip()
        return "" if text.lower() == "nan" else text

    def _build_query(item: dict[str, Any]) -> str:
        title = _clean(item.get("titulo"))
        if not title:
            return ""

        if str(item.get("entity_type") or "").strip().lower() == "place":
            district = _clean(item.get("distrito"))
            category = _clean(item.get("categoria_google") or item.get("tipo"))
            address = _clean(item.get("direccion"))
            parts = [title]
            if district:
                parts.append(district)
            if category:
                parts.append(category)
            if address:
                parts.append(address)
            return " | ".join(parts)

        # Para eventos, titulo solo produce colisiones en catalogos con replicas.
        # Se agrega contexto estable para desambiguar sin usar campos internos.
        category = _clean(item.get("tipo"))
        place = _clean(item.get("lugar"))
        date_start = _clean(item.get("fecha_inicio"))
        time_start = _clean(item.get("hora_inicio"))

        parts = [title]
        if category:
            parts.append(category)
        if place:
            parts.append(place)
        if date_start:
            parts.append(date_start)
        if time_start:
            parts.append(time_start)
        return " | ".join(parts)

    usable_indices = []
    for idx, item in enumerate(metadata):
        q = _build_query(item)
        if q:
            usable_indices.append(idx)
    if not usable_indices:
        return {"available": False, "reason": "sin titulos para consulta"}

    if sample_size > 0 and len(usable_indices) > sample_size:
        rng = np.random.default_rng(42)
        usable_indices = sorted(rng.choice(usable_indices, size=sample_size, replace=False).tolist())

    queries = [_build_query(metadata[idx]) for idx in usable_indices]

    model = SentenceTransformer(model_name)
    q_vectors = model.encode(
        queries,
        batch_size=64,
        show_progress_bar=False,
        normalize_embeddings=True,
    )

    if vectors.ndim != 2:
        return {"available": False, "reason": "vectores con forma invalida"}

    # Query-title retrieval against dataset vectors.
    sims = np.asarray(q_vectors, dtype=np.float32) @ np.asarray(vectors, dtype=np.float32).T
    order = np.argsort(sims, axis=1)[:, ::-1]

    hits_at_1 = 0
    hits_at_k = 0
    for row_idx, target_idx in enumerate(usable_indices):
        ranked = order[row_idx]
        if len(ranked) > 0 and int(ranked[0]) == target_idx:
            hits_at_1 += 1
        if target_idx in ranked[:top_k]:
            hits_at_k += 1

    total = len(usable_indices)
    return {
        "available": True,
        "sample_size": total,
        "top_k": top_k,
        "recall_at_1": hits_at_1 / total if total else None,
        "recall_at_k": hits_at_k / total if total else None,
        "model": model_name,
    }


def validate_embeddings(
    run_id: str,
    top_k: int,
    sample_size: int,
    dup_threshold: float,
    norm_tolerance: float,
) -> tuple[dict[str, Any], str]:
    ctx = _resolve_paths(run_id)
    vectors = np.load(ctx.vectors_path)
    metadata = _load_json(ctx.metadata_path)
    contract = _load_json(ctx.contract_path) if ctx.contract_path.exists() else {}

    n_vectors = int(vectors.shape[0]) if vectors.ndim >= 1 else 0
    dim = int(vectors.shape[1]) if vectors.ndim == 2 else None
    n_metadata = len(metadata) if isinstance(metadata, list) else 0

    finite_mask = np.isfinite(vectors)
    finite_ratio = float(finite_mask.mean()) if finite_mask.size else 0.0

    norms = np.linalg.norm(vectors, axis=1) if vectors.ndim == 2 and n_vectors else np.array([])
    unit_mask = np.abs(norms - 1.0) <= norm_tolerance if norms.size else np.array([])

    contract_dim = contract.get("embedding_dim")
    dim_matches_contract = (dim == int(contract_dim)) if contract_dim is not None and dim is not None else None

    similarities: dict[str, Any] = {
        "top1_mean": None,
        "top1_median": None,
        "top1_p95": None,
        "near_duplicate_pairs": 0,
        "near_duplicate_ratio": None,
        "examples": [],
    }

    if vectors.ndim == 2 and n_vectors > 1:
        sim_matrix = np.asarray(vectors, dtype=np.float32) @ np.asarray(vectors, dtype=np.float32).T
        np.fill_diagonal(sim_matrix, -np.inf)

        top1_scores = sim_matrix.max(axis=1)
        similarities["top1_mean"] = _safe_float(np.mean(top1_scores))
        similarities["top1_median"] = _safe_float(np.median(top1_scores))
        similarities["top1_p95"] = _safe_float(np.percentile(top1_scores, 95))

        upper = np.triu(sim_matrix, k=1)
        near_dup_positions = np.argwhere(upper >= dup_threshold)
        near_dup_count = int(near_dup_positions.shape[0])
        total_pairs = (n_vectors * (n_vectors - 1)) // 2
        near_dup_ratio = (near_dup_count / total_pairs) if total_pairs else 0.0
        similarities["near_duplicate_pairs"] = near_dup_count
        similarities["near_duplicate_ratio"] = near_dup_ratio

        examples = []
        for i, j in near_dup_positions[:10]:
            left = metadata[int(i)] if int(i) < n_metadata else {}
            right = metadata[int(j)] if int(j) < n_metadata else {}
            examples.append(
                {
                    "i": int(i),
                    "j": int(j),
                    "score": _safe_float(sim_matrix[int(i), int(j)]),
                    "left": {
                        "entity_id": left.get("entity_id"),
                        "titulo": left.get("titulo"),
                        "fuente": left.get("fuente"),
                    },
                    "right": {
                        "entity_id": right.get("entity_id"),
                        "titulo": right.get("titulo"),
                        "fuente": right.get("fuente"),
                    },
                }
            )
        similarities["examples"] = examples

    model_name = str(contract.get("embedding_model") or "paraphrase-multilingual-MiniLM-L12-v2")
    retrieval = _self_retrieval_metrics(
        vectors=vectors,
        metadata=metadata if isinstance(metadata, list) else [],
        model_name=model_name,
        top_k=top_k,
        sample_size=sample_size,
    )

    checks = {
        "vectors_shape_valid": vectors.ndim == 2 and n_vectors > 0 and (dim is not None and dim > 0),
        "metadata_len_matches_vectors": n_metadata == n_vectors,
        "finite_values_only": finite_ratio == 1.0,
        "contract_dim_matches": dim_matches_contract,
        "unit_norm_rate_ge_0_95": (float(unit_mask.mean()) >= 0.95) if unit_mask.size else False,
        "self_recall_at_k_ge_0_90": (
            bool(retrieval.get("available"))
            and retrieval.get("recall_at_k") is not None
            and float(retrieval["recall_at_k"]) >= 0.90
        ),
    }

    hard_fail = (
        not checks["vectors_shape_valid"]
        or not checks["metadata_len_matches_vectors"]
        or not checks["finite_values_only"]
        or checks["contract_dim_matches"] is False
    )

    warn = (
        not checks["unit_norm_rate_ge_0_95"]
        or (retrieval.get("available") and not checks["self_recall_at_k_ge_0_90"])
    )

    status = "FAIL" if hard_fail else ("WARN" if warn else "PASS")

    report = {
        "run_id": ctx.run_id,
        "validated_at_utc": datetime.utcnow().isoformat(),
        "status": status,
        "inputs": {
            "vectors_path": str(ctx.vectors_path),
            "metadata_path": str(ctx.metadata_path),
            "contract_path": str(ctx.contract_path),
        },
        "shape": {
            "n_vectors": n_vectors,
            "dim": dim,
            "metadata_records": n_metadata,
            "contract_dim": contract_dim,
        },
        "quality": {
            "finite_ratio": finite_ratio,
            "norm": {
                "mean": _safe_float(np.mean(norms)) if norms.size else None,
                "std": _safe_float(np.std(norms)) if norms.size else None,
                "min": _safe_float(np.min(norms)) if norms.size else None,
                "max": _safe_float(np.max(norms)) if norms.size else None,
                "unit_rate_tol": norm_tolerance,
                "unit_rate": _safe_float(np.mean(unit_mask)) if unit_mask.size else None,
            },
            "similarity": similarities,
            "self_retrieval": retrieval,
        },
        "checks": checks,
        "parameters": {
            "top_k": top_k,
            "sample_size": sample_size,
            "dup_threshold": dup_threshold,
            "norm_tolerance": norm_tolerance,
        },
    }

    return report, ctx.run_id


def save_report(report: dict[str, Any], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)


def main() -> None:
    parser = argparse.ArgumentParser(description="Valida calidad de embeddings semánticos")
    parser.add_argument("--run-id", default="latest", help="Run id objetivo (por defecto: latest)")
    parser.add_argument("--top-k", type=int, default=5, help="k para recall@k en self-retrieval")
    parser.add_argument(
        "--sample-size",
        type=int,
        default=300,
        help="cantidad de consultas para self-retrieval (0 = usar todo)",
    )
    parser.add_argument(
        "--dup-threshold",
        type=float,
        default=0.95,
        help="umbral de cosine para near-duplicates",
    )
    parser.add_argument(
        "--norm-tolerance",
        type=float,
        default=1e-3,
        help="tolerancia para considerar ||v|| ~= 1",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="ruta de salida opcional del reporte JSON",
    )
    args = parser.parse_args()

    report, resolved_run_id = validate_embeddings(
        run_id=args.run_id,
        top_k=args.top_k,
        sample_size=args.sample_size,
        dup_threshold=args.dup_threshold,
        norm_tolerance=args.norm_tolerance,
    )

    default_output = EMBEDDINGS_DIR / f"quality_{resolved_run_id}.json"
    output_path = Path(args.output) if args.output else default_output
    save_report(report, output_path)

    latest = EMBEDDINGS_DIR / "quality_latest.json"
    if latest.exists() or latest.is_symlink():
        latest.unlink()
    latest.symlink_to(output_path.name)

    print("[validate_embeddings] Resumen")
    print(f"  - status: {report['status']}")
    print(f"  - run_id: {resolved_run_id}")
    print(f"  - vectores: {report['shape']['n_vectors']} x {report['shape']['dim']}")
    print(f"  - metadata: {report['shape']['metadata_records']}")
    print(f"  - norm.mean: {report['quality']['norm']['mean']}")
    print(f"  - norm.unit_rate: {report['quality']['norm']['unit_rate']}")
    print(f"  - near_duplicates(>=th): {report['quality']['similarity']['near_duplicate_pairs']}")

    retrieval = report["quality"]["self_retrieval"]
    if retrieval.get("available"):
        print(f"  - self_recall@1: {retrieval.get('recall_at_1')}")
        print(f"  - self_recall@{retrieval.get('top_k')}: {retrieval.get('recall_at_k')}")
    else:
        print(f"  - self_retrieval: skipped ({retrieval.get('reason')})")

    print(f"  - reporte: {output_path}")


if __name__ == "__main__":
    main()
