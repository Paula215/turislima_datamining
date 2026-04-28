# =============================================================================
# Cultural Pipeline — Container Apps Job image
# =============================================================================
# Multi-stage build:
#   - builder: instala Python deps (incl. Azure SDKs) + pre-descarga el modelo
#     paraphrase-multilingual-MiniLM-L12-v2 para evitar downloads en cada run.
#   - runtime: imagen final con chromium + chromium-driver para Selenium
#     (MALI / Joinnus), copiando solo el código y los artefactos del builder.
#
# ENTRYPOINT por defecto: `python pipeline/pipeline.py --stage=all`.
# Override desde Container Apps Job con --command/--args si se requiere.
# =============================================================================

ARG PYTHON_VERSION=3.11

# ----------------------------- builder ---------------------------------------

FROM python:${PYTHON_VERSION}-slim AS builder

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    HF_HOME=/opt/hf-cache \
    SENTENCE_TRANSFORMERS_HOME=/opt/hf-cache/sentence-transformers

WORKDIR /build

# Build deps (compilers used by pyarrow/numpy wheels en arquitecturas no-amd64)
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt requirements-azure.txt ./
RUN pip install --user -r requirements.txt -r requirements-azure.txt

# Pre-descarga el modelo embedding (~470MB) para evitar HF rate-limits y
# acelerar cold start del job.
ENV PATH=/root/.local/bin:${PATH}
RUN python - <<'PY'
from sentence_transformers import SentenceTransformer
SentenceTransformer("paraphrase-multilingual-MiniLM-L12-v2")
PY

# ----------------------------- runtime ---------------------------------------

FROM python:${PYTHON_VERSION}-slim AS runtime

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    HF_HOME=/opt/hf-cache \
    SENTENCE_TRANSFORMERS_HOME=/opt/hf-cache/sentence-transformers \
    USE_SYSTEM_CHROMEDRIVER=true \
    PATH=/root/.local/bin:${PATH}

# Chromium para Selenium (MALI + bootstrap auth Joinnus).
# `chromium-driver` deja chromedriver en /usr/bin/chromedriver y Selenium
# Manager (>=4.6) lo auto-descubre cuando USE_SYSTEM_CHROMEDRIVER=true.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        chromium \
        chromium-driver \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copia las deps Python y el cache HF del builder
COPY --from=builder /root/.local /root/.local
COPY --from=builder /opt/hf-cache /opt/hf-cache

# Código de la app
COPY pipeline/    /app/pipeline/
COPY scrapers/    /app/scrapers/
COPY embeddings/  /app/embeddings/
COPY scripts/     /app/scripts/
COPY input/       /app/input/

# Directorios runtime (LakeBackend=local los reusa si LAKE_LOCAL_ROOT=/app/data)
RUN mkdir -p /app/data /app/logs /app/output

# El job no expone puertos; el runner lo dispara y termina.
ENTRYPOINT ["python", "/app/pipeline/pipeline.py"]
CMD ["--stage=all"]
