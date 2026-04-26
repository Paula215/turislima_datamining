"""
build_faiss_index.py
====================
Lee poi_catalog.json, extrae los embeddings y construye un índice FAISS
IndexFlatIP (producto interno = coseno si los vectores están normalizados).

Archivos de salida en el mismo directorio que poi_catalog.json:
    faiss_index.bin   — índice FAISS serializado
    id_map.json       — {"0": "poi_abc123", "1": "poi_def456", ...}
                        mapeo posición en el índice → poi_id

Uso standalone:
    python build_faiss_index.py --catalog-dir ../output
"""

import argparse
import json
import numpy as np
from pathlib import Path

CATALOG_FILENAME = "poi_catalog.json"
INDEX_FILENAME = "faiss_index.bin"
ID_MAP_FILENAME = "id_map.json"


def build_index(output_dir: Path) -> tuple[int, Path, Path]:
    """
    Construye el índice FAISS desde poi_catalog.json.

    Registros con embedding=null se omiten del índice (se loggean).
    Lanza RuntimeError si faiss-cpu no está instalado.
    Lanza FileNotFoundError si poi_catalog.json no existe.
    Lanza ValueError si ningún registro tiene embedding.

    Returns (n_indexed, index_path, id_map_path).
    """
    try:
        import faiss
    except ImportError:
        raise RuntimeError(
            "faiss-cpu no instalado. Ejecuta: pip install faiss-cpu"
        )

    output_dir = Path(output_dir)
    catalog_path = output_dir / CATALOG_FILENAME

    if not catalog_path.exists():
        raise FileNotFoundError(
            f"No se encontró {catalog_path}. "
            "Ejecuta el pipeline completo antes de construir el índice."
        )

    with open(catalog_path, "r", encoding="utf-8") as f:
        records = json.load(f)

    if not records:
        raise ValueError("poi_catalog.json está vacío.")

    # ── Extraer embeddings ────────────────────────────────────────────────────
    vectors: list[list[float]] = []
    id_map: dict[str, str] = {}
    skipped = 0

    for rec in records:
        emb = rec.get("embedding")
        if emb is None:
            skipped += 1
            continue
        pos = str(len(vectors))
        id_map[pos] = rec["poi_id"]
        vectors.append(emb)

    if not vectors:
        raise ValueError(
            "Ningún registro en poi_catalog.json tiene embedding. "
            "Ejecuta el pipeline sin --skip-embeddings."
        )

    if skipped:
        print(f"  ⚠️  {skipped}/{len(records)} registros sin embedding omitidos del índice")

    # ── Construir índice ──────────────────────────────────────────────────────
    matrix = np.array(vectors, dtype=np.float32)
    n, dim = matrix.shape
    print(f"  📐 Matriz de embeddings: {n} vectores × {dim} dimensiones")

    index = faiss.IndexFlatIP(dim)
    index.add(matrix)

    # ── Validación ────────────────────────────────────────────────────────────
    n_catalog = len(records)
    n_indexed = index.ntotal

    if n_indexed != n_catalog:
        print(
            f"  ⚠️  Validación: índice={n_indexed} ≠ catalog={n_catalog} "
            f"({n_catalog - n_indexed} registros sin embedding)"
        )
    else:
        print(f"  ✅ Validación OK: {n_indexed} entradas == {n_catalog} registros en catalog")

    assert n_indexed == n, f"FAISS ntotal={n_indexed} no coincide con vectores cargados={n}"

    # ── Guardar ───────────────────────────────────────────────────────────────
    index_path = output_dir / INDEX_FILENAME
    faiss.write_index(index, str(index_path))
    print(f"  💾 faiss_index.bin: {index_path}")

    id_map_path = output_dir / ID_MAP_FILENAME
    with open(id_map_path, "w", encoding="utf-8") as f:
        json.dump(id_map, f, ensure_ascii=False, separators=(",", ":"))
    print(f"  🗺️  id_map.json:    {id_map_path}  ({len(id_map)} entradas)")

    return n_indexed, index_path, id_map_path


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Construye índice FAISS desde poi_catalog.json")
    parser.add_argument(
        "--catalog-dir",
        default=str(Path(__file__).parent.parent / "output"),
        help="Directorio que contiene poi_catalog.json (default: ../output)",
    )
    args = parser.parse_args()
    n, idx_path, map_path = build_index(Path(args.catalog_dir))
    print(f"\n✅ Índice listo: {n} vectores en {idx_path}")
