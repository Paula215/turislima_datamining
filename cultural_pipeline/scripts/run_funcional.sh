#!/usr/bin/env bash
set -euo pipefail

# Functional-first runner:
# - Runs a minimal pipeline path that should work before Mongo/CI fine-tuning.
# - Validates generated outputs.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

PYTHON_BIN="${PYTHON_BIN:-python}"
SOURCES="${SOURCES:-bnp joinnus mali}"
DRY_RUN="${DRY_RUN:-false}"
SKIP_EMBEDDINGS="${SKIP_EMBEDDINGS:-true}"
WRITE_MONGO_WEB="${WRITE_MONGO_WEB:-false}"
WRITE_MONGO_RECO="${WRITE_MONGO_RECO:-false}"

CMD=("$PYTHON_BIN" "pipeline/pipeline.py")

if [[ "$DRY_RUN" == "true" ]]; then
  CMD+=("--dry-run")
fi

if [[ -n "$SOURCES" ]]; then
  read -r -a SRC_ARRAY <<< "$SOURCES"
  CMD+=("--sources")
  CMD+=("${SRC_ARRAY[@]}")
fi

if [[ "$SKIP_EMBEDDINGS" == "true" ]]; then
  CMD+=("--skip-embeddings")
fi

if [[ "$WRITE_MONGO_WEB" == "true" ]]; then
  CMD+=("--write-mongo-web")
fi

if [[ "$WRITE_MONGO_RECO" == "true" ]]; then
  CMD+=("--write-mongo-reco")
fi

echo "[run_funcional] Ejecutando: ${CMD[*]}"
"${CMD[@]}"

echo "[run_funcional] Validando outputs..."
"$PYTHON_BIN" scripts/validar_output.py \
  --expected-sources ${SOURCES} \
  --emit-json

echo "[run_funcional] OK: pipeline funcional validado"
