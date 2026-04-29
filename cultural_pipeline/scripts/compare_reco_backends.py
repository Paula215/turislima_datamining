"""
compare_reco_backends.py
========================
Paridad top-k entre el recomendador legacy (FAISS local) y el nuevo
(Cosmos vCore vector search). Usado durante la Fase 1 del cutover
(`docs/plans/medallion-cutover-plan.md`) antes de flipear el
`RECO_BACKEND` del lado lector.

Para cada query del fixture (`cutover_queries.json` por defecto):
  - encodea con `paraphrase-multilingual-MiniLM-L12-v2`
  - corre top-k contra FAISS (id_map.json → poi_id → entity_id)
  - corre top-k contra Cosmos vCore (`cosmos_sink.search_similar`)
  - reporta Jaccard@k y la lista de hits de cada backend

Salidas:
  - logs/cutover_parity_<timestamp>.json (si --emit-json)
  - stdout: tabla por query + agregado

Pre-requisitos:
  - poi_catalog.json + faiss_index.bin en `output/`
  - Cosmos accesible (COSMOS_URI o Key Vault)
  - sentence-transformers instalado
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean
from typing import Any

import numpy as np

ROOT = Path(__file__).resolve().parent.parent
LOGS = ROOT / "logs"
OUTPUT = ROOT / "output"

sys.path.insert(0, str(ROOT / "pipeline"))
sys.path.insert(0, str(ROOT / "scrapers"))
sys.path.insert(0, str(ROOT / "embeddings"))


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _build_poi_to_entity_map() -> dict[str, dict[str, Any]]:
    """Devuelve {poi_id: {entity_id, titulo, ...}} desde poi_catalog.json."""
    catalog_path = OUTPUT / "poi_catalog.json"
    if not catalog_path.exists():
        raise FileNotFoundError(f"No existe {catalog_path}")
    with open(catalog_path, "r", encoding="utf-8") as f:
        records = json.load(f)
    out: dict[str, dict[str, Any]] = {}
    for rec in records:
        poi_id = rec.get("poi_id")
        if not poi_id:
            continue
        out[str(poi_id)] = {
            "entity_id": rec.get("entity_id"),
            "titulo": rec.get("titulo"),
            "fuente": rec.get("fuente"),
            "tipo": rec.get("tipo"),
        }
    return out


def _load_faiss_index():
    try:
        import faiss
    except ImportError as exc:
        raise RuntimeError("faiss-cpu no instalado") from exc
    index_path = OUTPUT / "faiss_index.bin"
    id_map_path = OUTPUT / "id_map.json"
    if not index_path.exists() or not id_map_path.exists():
        raise FileNotFoundError(
            f"Faltan {index_path} o {id_map_path} — corre el pipeline completo primero"
        )
    index = faiss.read_index(str(index_path))
    with open(id_map_path, "r", encoding="utf-8") as f:
        id_map = json.load(f)
    return index, id_map


def _faiss_top_k(model, query: str, index, id_map, top_k: int) -> list[str]:
    """Devuelve top-k poi_id, ordenados por score descendente."""
    vec = model.encode([query], normalize_embeddings=True).astype(np.float32)
    scores, idxs = index.search(vec, top_k)
    out = []
    for pos in idxs[0]:
        poi = id_map.get(str(int(pos)))
        if poi:
            out.append(poi)
    return out


def _cosmos_top_k(model, query: str, top_k: int) -> list[dict[str, Any]]:
    """Devuelve top-k docs de cosmos_sink.search_similar, en orden."""
    import cosmos_sink  # type: ignore[import-not-found]

    vec = model.encode([query], normalize_embeddings=True).astype(np.float32)[0]
    return cosmos_sink.search_similar(query_embedding=vec.tolist(), top_k=top_k)


def _jaccard(a: set[str], b: set[str]) -> float:
    if not a and not b:
        return 1.0
    union = a | b
    if not union:
        return 1.0
    return len(a & b) / len(union)


def compare(
    queries: list[dict[str, Any]],
    top_k: int,
    threshold: float,
) -> dict[str, Any]:
    from sentence_transformers import SentenceTransformer  # noqa: E402

    poi_map = _build_poi_to_entity_map()
    faiss_index, id_map = _load_faiss_index()
    model = SentenceTransformer("paraphrase-multilingual-MiniLM-L12-v2")

    per_query: list[dict[str, Any]] = []
    jaccards: list[float] = []
    pass_count = 0

    for entry in queries:
        q = entry["q"]
        # FAISS → poi_ids → entity_ids via poi_map
        faiss_pois = _faiss_top_k(model, q, faiss_index, id_map, top_k)
        faiss_entity_ids = {
            poi_map[p].get("entity_id")
            for p in faiss_pois
            if p in poi_map and poi_map[p].get("entity_id")
        }
        faiss_titulos = [
            poi_map[p].get("titulo") for p in faiss_pois if p in poi_map
        ]

        # Cosmos → docs con entity_id
        cosmos_docs = _cosmos_top_k(model, q, top_k)
        cosmos_entity_ids = {
            d["entity_id"] for d in cosmos_docs if d.get("entity_id")
        }
        cosmos_titulos = [d.get("titulo") for d in cosmos_docs]

        j = _jaccard(faiss_entity_ids, cosmos_entity_ids)
        jaccards.append(j)
        if j >= threshold:
            pass_count += 1

        per_query.append(
            {
                "query": q,
                "jaccard_at_k": round(j, 4),
                "faiss_top_k": faiss_titulos,
                "cosmos_top_k": cosmos_titulos,
                "faiss_entity_ids": list(faiss_entity_ids),
                "cosmos_entity_ids": list(cosmos_entity_ids),
            }
        )

    aggregate = {
        "queries": len(queries),
        "top_k": top_k,
        "threshold": threshold,
        "jaccard_mean": round(mean(jaccards), 4) if jaccards else None,
        "jaccard_min": round(min(jaccards), 4) if jaccards else None,
        "jaccard_max": round(max(jaccards), 4) if jaccards else None,
        "queries_above_threshold": pass_count,
        "queries_below_threshold": len(jaccards) - pass_count,
    }
    status = (
        "PASS"
        if aggregate["jaccard_mean"] is not None
        and aggregate["jaccard_mean"] >= threshold
        else "WARN"
    )

    return {
        "validated_at_utc": _utcnow_iso(),
        "status": status,
        "aggregate": aggregate,
        "per_query": per_query,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compara paridad top-k entre FAISS legacy y Cosmos vCore"
    )
    parser.add_argument(
        "--queries",
        default=str(ROOT / "scripts" / "cutover_queries.json"),
        help="Archivo JSON con [{q, filter}, ...]",
    )
    parser.add_argument("--top-k", type=int, default=10)
    parser.add_argument(
        "--threshold",
        type=float,
        default=0.7,
        help="Jaccard mínimo aceptable (default 0.7)",
    )
    parser.add_argument(
        "--emit-json",
        action="store_true",
        help="Escribe reporte a logs/cutover_parity_<run>.json",
    )
    args = parser.parse_args()

    with open(args.queries, "r", encoding="utf-8") as f:
        queries = json.load(f)

    report = compare(queries=queries, top_k=args.top_k, threshold=args.threshold)

    print(f"[compare_reco_backends] {report['status']}")
    agg = report["aggregate"]
    print(
        f"  - queries: {agg['queries']} · top_k: {agg['top_k']} · threshold: {agg['threshold']}"
    )
    print(
        f"  - jaccard mean/min/max: "
        f"{agg['jaccard_mean']} / {agg['jaccard_min']} / {agg['jaccard_max']}"
    )
    print(
        f"  - queries ≥ threshold: {agg['queries_above_threshold']} / {agg['queries']}"
    )
    print()
    print("  per-query (top 10 por jaccard descendente):")
    sorted_q = sorted(
        report["per_query"], key=lambda r: r["jaccard_at_k"], reverse=True
    )
    for item in sorted_q[:10]:
        print(f"    [{item['jaccard_at_k']:.2f}] {item['query']}")

    if args.emit_json:
        LOGS.mkdir(exist_ok=True)
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        path = LOGS / f"cutover_parity_{ts}.json"
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(report, fh, ensure_ascii=False, indent=2)
        print(f"\n  reporte JSON: {path}")

    sys.exit(0 if report["status"] == "PASS" else 1)


if __name__ == "__main__":
    main()
