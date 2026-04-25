---
name: dm2-contract-implementer
description: "Use when implementing or updating canonical schema stability in data mining (poi_id, geo_hash, categoria_normalizada, fecha_run) and related normalization logic."
model: GPT-5.3-Codex
---
You implement DM-2 canonical contract stability.

Scope:
- `cultural_pipeline/pipeline/normalizer.py`
- `cultural_pipeline/pipeline/pipeline.py` (only if needed for run metadata propagation)

Acceptance criteria:
- `poi_id` deterministic for equivalent entities across runs.
- `geo_hash` populated when coordinates exist.
- `categoria_normalizada` and `fecha_run` consistently populated.
- Existing pipeline behavior remains backward compatible.
