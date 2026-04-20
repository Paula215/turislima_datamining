"""
embedder.py
===========
Genera embeddings semánticos para cada evento cultural.

Estrategia:
  1. Intenta usar sentence-transformers (modelo local, sin costo)
     Modelo: "paraphrase-multilingual-MiniLM-L12-v2"  — soporta español
  2. Fallback: OpenAI text-embedding-3-small (requiere OPENAI_API_KEY)
  3. Guarda los vectores en:
     - embeddings/vectors.npy        — matriz NumPy (N, D)
     - embeddings/metadata.json      — {event_id, titulo, fuente, ...}
     - embeddings/faiss.index        — índice FAISS para búsqueda rápida (si disponible)

El campo `texto_embedding` del esquema estándar es el texto de entrada.
Está construido para capturar:
  - Título (qué es el evento)
  - Tipo / categoría (¿concierto? ¿exposición? ¿taller?)
  - Lugar (¿dónde está?)
  - Descripción (el contenido semántico más rico)
  - Tags / performers (señales adicionales)
  - Precio (gratuito vs pago)

Esto permite queries como:
  "Quiero algo cultural y gratuito este fin de semana"
  "Busco un concierto de jazz en Miraflores"
  "Actividades para niños en Lima"
"""

import os
import json
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Optional

OUTPUT_DIR = Path(__file__).parent.parent / "embeddings"
OUTPUT_DIR.mkdir(exist_ok=True)


# ---------------------------------------------------------------------------
# Embedding contract (freeze for production)
# ---------------------------------------------------------------------------

EMBEDDING_MODEL_NAME = os.getenv("EMBEDDING_MODEL_NAME") or "paraphrase-multilingual-MiniLM-L12-v2"
EMBEDDING_DIM = int(os.getenv("EMBEDDING_DIM") or "384")


def _bool_env(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    return raw.strip().lower() in ("1", "true", "yes", "y", "on")


# ---------------------------------------------------------------------------
# Backend 1: sentence-transformers (local, multilingüe, recomendado)
# ---------------------------------------------------------------------------

def embed_with_sentence_transformers(texts: list[str], batch_size: int = 32) -> np.ndarray:
    from sentence_transformers import SentenceTransformer
    print(f"  🤖 Usando sentence-transformers ({EMBEDDING_MODEL_NAME})")
    model = SentenceTransformer(EMBEDDING_MODEL_NAME)
    embeddings = model.encode(
        texts,
        batch_size=batch_size,
        show_progress_bar=True,
        normalize_embeddings=True,   # cosine similarity listo
    )
    return embeddings


# ---------------------------------------------------------------------------
# Backend 2: OpenAI (fallback si no hay GPU / sentence-transformers)
# ---------------------------------------------------------------------------

def embed_with_openai(texts: list[str], batch_size: int = 100) -> np.ndarray:
    import openai
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY no configurado")

    client = openai.OpenAI(api_key=api_key)
    openai_model = os.getenv("OPENAI_EMBEDDING_MODEL") or "text-embedding-3-small"
    print(f"  🌐 Usando OpenAI {openai_model}")

    all_embeddings = []
    for i in range(0, len(texts), batch_size):
        batch = texts[i : i + batch_size]
        response = client.embeddings.create(
            model=openai_model,
            input=batch,
        )
        vecs = [item.embedding for item in response.data]
        all_embeddings.extend(vecs)

    return np.array(all_embeddings, dtype=np.float32)


# ---------------------------------------------------------------------------
# Guardar embeddings + metadata + FAISS index
# ---------------------------------------------------------------------------

def save_embeddings(embeddings: np.ndarray, df: pd.DataFrame, run_id: str):
    # Vectores
    vectors_path = OUTPUT_DIR / f"vectors_{run_id}.npy"
    np.save(vectors_path, embeddings)
    print(f"  💾 Vectores guardados: {vectors_path}  shape={embeddings.shape}")

    # Metadata
    meta_cols = [
        "entity_id", "entity_type", "event_id", "titulo", "tipo", "fecha_inicio", "lugar",
        "precio", "fuente", "ciudad", "url_origen", "url_evento", "imagen_url",
        "direccion", "distrito", "categoria_google", "rating", "ratings_total", "lat", "lng",
        "resumen_reviews", "descripcion", "tags", "texto_embedding"
    ]
    available = [c for c in meta_cols if c in df.columns]
    meta = df[available].to_dict(orient="records")
    meta_path = OUTPUT_DIR / f"metadata_{run_id}.json"
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2, default=str)
    print(f"  📋 Metadata guardada: {meta_path}")

    # Contract / provenance
    contract = {
        "run_id": run_id,
        "embedding_model": EMBEDDING_MODEL_NAME,
        "embedding_dim": int(embeddings.shape[1]) if embeddings.ndim == 2 else None,
        "embedding_similarity": "cosine",
        "normalize_embeddings": True,
        "generated_at_utc": datetime.utcnow().isoformat(),
    }
    contract_path = OUTPUT_DIR / f"contract_{run_id}.json"
    with open(contract_path, "w", encoding="utf-8") as f:
        json.dump(contract, f, ensure_ascii=False, indent=2)
    print(f"  🧾 Contract guardado: {contract_path}")

    # FAISS index (opcional)
    try:
        import faiss
        dim = embeddings.shape[1]
        index = faiss.IndexFlatIP(dim)   # Inner Product (cosine si vectores normalizados)
        index.add(embeddings.astype(np.float32))
        faiss_path = OUTPUT_DIR / f"faiss_{run_id}.index"
        faiss.write_index(index, str(faiss_path))
        print(f"  🔍 FAISS index guardado: {faiss_path}  ({index.ntotal} vectores)")
    except ImportError:
        print("  ⚠️ FAISS no instalado — se omite el índice de búsqueda")

    # Symlinks latest
    for fname, path in [
        ("vectors_latest.npy", vectors_path),
        ("metadata_latest.json", meta_path),
        ("contract_latest.json", contract_path),
    ]:
        latest = OUTPUT_DIR / fname
        if latest.exists() or latest.is_symlink():
            latest.unlink()
        latest.symlink_to(path.name)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def generate_embeddings(df: pd.DataFrame, run_id: Optional[str] = None) -> np.ndarray:
    if df.empty or "texto_embedding" not in df.columns:
        print("⚠️ DataFrame vacío o sin columna texto_embedding")
        return np.array([])

    if run_id is None:
        run_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    texts = df["texto_embedding"].fillna("").tolist()
    print(f"🔤 Generando embeddings para {len(texts)} eventos...")

    allow_openai_fallback = _bool_env("ALLOW_OPENAI_FALLBACK", default=False)

    # Intenta backend 1, cae a backend 2 solo si está permitido
    try:
        embeddings = embed_with_sentence_transformers(texts)
    except ImportError:
        if not allow_openai_fallback:
            raise RuntimeError(
                "sentence-transformers no disponible y ALLOW_OPENAI_FALLBACK=false. "
                "Instala sentence-transformers o habilita el fallback explícitamente."
            )
        print("  sentence-transformers no disponible, intentando OpenAI...")
        embeddings = embed_with_openai(texts)

    # Dimension guardrail (prevents index breakage)
    if embeddings.ndim != 2 or embeddings.shape[1] != EMBEDDING_DIM:
        raise RuntimeError(
            f"Dimensión de embedding inesperada: got={embeddings.shape} expected=(*,{EMBEDDING_DIM}). "
            "Revisa EMBEDDING_DIM/EMBEDDING_MODEL_NAME o el backend de embeddings."
        )

    save_embeddings(embeddings, df, run_id)
    print(f"✅ Embeddings generados: dim={embeddings.shape}")
    return embeddings


# ---------------------------------------------------------------------------
# Búsqueda semántica de prueba
# ---------------------------------------------------------------------------

def search_events(query: str, top_k: int = 5, run_id: str = "latest") -> list[dict]:
    """
    Busca los top_k eventos más similares a un query de texto libre.
    Requiere que los embeddings ya estén generados.
    """
    vectors_path = OUTPUT_DIR / f"vectors_{run_id}.npy"
    meta_path = OUTPUT_DIR / f"metadata_{run_id}.json"

    if not vectors_path.exists():
        raise FileNotFoundError(f"No se encontraron vectores en {vectors_path}")

    vectors = np.load(vectors_path)

    with open(meta_path, "r", encoding="utf-8") as f:
        metadata = json.load(f)

    # Embed el query
    try:
        from sentence_transformers import SentenceTransformer
        model = SentenceTransformer(EMBEDDING_MODEL_NAME)
        q_vec = model.encode([query], normalize_embeddings=True)[0]
    except ImportError:
        q_vec = embed_with_openai([query])[0]

    # Cosine similarity (dot product si vectores normalizados)
    scores = vectors @ q_vec
    top_indices = np.argsort(scores)[::-1][:top_k]

    results = []
    for idx in top_indices:
        item = metadata[idx].copy()
        item["score"] = float(scores[idx])
        results.append(item)

    return results


if __name__ == "__main__":
    # Demo: carga datos de muestra y genera embeddings
    sample_data = pd.DataFrame([{
        "event_id": "test_001",
        "titulo": "Concierto de Jazz en Miraflores",
        "tipo": "concierto",
        "lugar": "Centro Cultural Miraflores",
        "descripcion": "Una noche de jazz con los mejores músicos de Lima",
        "precio": "Pago",
        "fuente": "demo",
        "ciudad": "Lima",
        "url_evento": "https://example.com",
        "imagen_url": None,
        "tags": ["jazz", "música", "nocturno"],
        "texto_embedding": "Evento: Concierto de Jazz en Miraflores | Tipo: concierto | Lugar: Centro Cultural Miraflores | Ciudad: Lima, Perú | Descripción: Una noche de jazz con los mejores músicos de Lima | Precio: Pago | Tags: jazz, música, nocturno",
    }])
    generate_embeddings(sample_data, run_id="demo")
