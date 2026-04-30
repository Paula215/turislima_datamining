# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

<!-- BEGIN BEADS INTEGRATION v:1 profile:minimal hash:ca08a54f -->
## Beads Issue Tracker

This project uses **bd (beads)** for issue tracking. Run `bd prime` to see full workflow context and commands.

### Quick Reference

```bash
bd ready              # Find available work
bd show <id>          # View issue details
bd update <id> --claim  # Claim work
bd close <id>         # Complete work
```

### Rules

- Use `bd` for ALL task tracking — do NOT use TodoWrite, TaskCreate, or markdown TODO lists
- Run `bd prime` for detailed command reference and session close protocol
- Use `bd remember` for persistent knowledge — do NOT use MEMORY.md files

## Session Completion

**When ending a work session**, you MUST complete ALL steps below. Work is NOT complete until `git push` succeeds.

**MANDATORY WORKFLOW:**

1. **File issues for remaining work** - Create issues for anything that needs follow-up
2. **Run quality gates** (if code changed) - Tests, linters, builds
3. **Update issue status** - Close finished work, update in-progress items
4. **PUSH TO REMOTE** - This is MANDATORY:
   ```bash
   git pull --rebase
   bd dolt push
   git push
   git status  # MUST show "up to date with origin"
   ```
5. **Clean up** - Clear stashes, prune remote branches
6. **Verify** - All changes committed AND pushed
7. **Hand off** - Provide context for next session

**CRITICAL RULES:**
- Work is NOT complete until `git push` succeeds
- NEVER stop before pushing - that leaves work stranded locally
- NEVER say "ready to push when you are" - YOU must push
- If push fails, resolve and retry until it succeeds
<!-- END BEADS INTEGRATION -->


## Build & Test

All commands run from `cultural_pipeline/`:

```bash
# Install dependencies
pip install -r requirements.txt

# Functional test (recommended first run — skips embeddings by default)
PYTHON_BIN=python bash scripts/run_funcional.sh

# Full pipeline
python pipeline/pipeline.py

# Common flags
python pipeline/pipeline.py --dry-run              # Use existing data, no scraping
python pipeline/pipeline.py --sources bnp joinnus  # Subset of sources
python pipeline/pipeline.py --skip-embeddings      # Skip vector generation (faster)
python pipeline/pipeline.py --write-mongo-web      # Publish to MongoDB web catalog
python pipeline/pipeline.py --write-mongo-reco     # Publish vectors to MongoDB reco

# Validate each medallion layer
python scripts/validate_bronze.py --run-id latest
python scripts/validate_silver.py --run-id latest
python scripts/validate_gold.py   --run-id latest

# Legacy output validation
python scripts/validar_output.py --expected-sources bnp mali joinnus places --emit-json
python scripts/validate_embeddings.py --run-id latest

# Compare Atlas vs Cosmos recommendations (parity check before cutover)
python scripts/compare_reco_backends.py --user-id <uid> --top-k 20

# Semantic search smoke test
python -c "
from embeddings.embedder import search_events
for r in search_events('jazz en vivo Lima', top_k=3):
    print(r['titulo'], r['score'])
"
```

There are no unit tests or linters configured. Quality is enforced through the validation scripts above.

## Architecture Overview

**Purpose**: Weekly data pipeline that extracts cultural events from Lima, Peru, normalizes them into a unified schema, generates semantic embeddings, and publishes to MongoDB for a tourism recommendation app.

### Data flow — Medallion architecture

The pipeline has two modes:

**Legacy path** (still default):
```
Scrapers → normalizer.py → output/eventos_estandar.{csv,json}
         → embedder.py   → embeddings/vectors_latest.npy + faiss_latest.index
         → mongo_sink.py (or cosmos_sink.py) → turislima.{entities, entities_vectors}
```

**Medallion path** (via `pipeline/stages/`):
```
Scrapers
  → stages/bronze.py  → lake://bronze/source=<name>/run_id=<id>/events.jsonl
  → stages/silver.py  → lake://silver/run_id=<id>/eventos_estandar.parquet  (canonical)
  → stages/gold.py    → lake://gold/run_id=<id>/{catalog.parquet, vectors.parquet}
                      → sinks: mongo_sink / cosmos_sink / FAISS (legacy)
```

The lake backend is swappable: `LAKE_BACKEND=local` writes under `cultural_pipeline/data/`; `LAKE_BACKEND=azure` writes to ADLS Gen2.

### Key modules

| Path | Role |
|------|------|
| `pipeline/pipeline.py` | Main orchestrator; CLI entry point; runs scrapers in parallel (max_workers=2) |
| `pipeline/normalizer.py` | Transforms raw scraper DataFrames into the unified `EventoEstandar` schema |
| `pipeline/mongo_sink.py` | Upserts to MongoDB Atlas; manages stale-document cleanup with a safety window |
| `pipeline/cosmos_sink.py` | Mirror of `mongo_sink.py` for Cosmos DB vCore; selected via `RECO_BACKEND=cosmos` |
| `pipeline/geocoder.py` | Enriches lat/lng for events missing coordinates (Google Geocoding API) |
| `pipeline/catalog_exporter.py` | Exports the canonical catalog to CSV/JSON/Parquet for downstream consumers |
| `pipeline/build_faiss_index.py` | Opt-in utility to build a FAISS index file from stored vectors |
| `pipeline/stages/bronze.py` | Medallion Bronze stage: runs scrapers, persists raw JSONL to lake |
| `pipeline/stages/silver.py` | Medallion Silver stage: normalizes raw data → typed Parquet (`EventoEstandar`) |
| `pipeline/stages/gold.py` | Medallion Gold stage: embeddings, catalog export, sink to Mongo/Cosmos |
| `pipeline/storage/_protocol.py` | `BaseLakeStore` ABC + `get_store()` factory — defines medallion-aware `write_bronze/silver/gold` methods |
| `pipeline/storage/_local.py` | Local filesystem lake backend |
| `pipeline/storage/_azure.py` | ADLS Gen2 lake backend (Azure) |
| `pipeline/contracts/` | Pydantic schemas for each layer: `silver_schema.py`, `gold_schema.py`; path layout in `layout.py`; run manifests in `manifests.py` |
| `scrapers/scraper_bnp.py` | HTML scraper for Biblioteca Nacional del Perú |
| `scrapers/scraper_mali.py` | Selenium-based scraper for Museo de Arte de Lima |
| `scrapers/scraper_joinnus.py` | REST API scraper for Joinnus ticketing (~40 KB, most complex) |
| `scrapers/scraper_google_places.py` | Loads static `input/google_places_payload.json` (deterministic, no live API) |
| `embeddings/embedder.py` | Generates vectors with `paraphrase-multilingual-MiniLM-L12-v2`; OpenAI fallback |
| `embeddings/enricher.py` | Optional DeepSeek enrichment of event descriptions |
| `scripts/validate_bronze.py` | Validates Bronze layer: JSONL structure, required fields, source completeness |
| `scripts/validate_silver.py` | Validates Silver layer: Parquet schema, nulls, duplicates, EventoEstandar contract |
| `scripts/validate_gold.py` | Validates Gold layer: embedding shape/norms, catalog parity, vector count |
| `scripts/validate_embeddings.py` | Embedding quality: L2 norms, self_recall@k, near-duplicates, drift vs prior run |
| `scripts/validar_output.py` | Legacy schema validation on CSV/JSON output |
| `scripts/compare_reco_backends.py` | Top-K parity check between Atlas and Cosmos vCore recommendations |

### Scheduling

The pipeline runs weekly (Sunday 07:00 UTC = 02:00 AM Lima). Three options:
- **GitHub Actions** (primary): `.github/workflows/weekly_pipeline.yml`
- **Crontab**: `python scheduler/scheduler.py --install-cron`
- **Daemon**: `python scheduler/scheduler.py`

### MongoDB / Cosmos routing

`RECO_BACKEND` env var controls which sink receives the Gold layer:
- `atlas` (default) → `mongo_sink.py` → `turislima.entities` and `turislima.entities_vectors`
- `cosmos` → `cosmos_sink.py` → Cosmos DB vCore (uses `COSMOS_URI` or Azure Key Vault secret `cosmos-uri`)

Both sinks expose the same public API (`upsert_events_web`, `upsert_events_reco`, `mark_inactive_*`, `delete_not_seen_*`) so `gold.py` doesn't need to know which is active.

The backend (`frontend_turislima/backend`) reads from the unified Atlas cluster (`turislima` DB). Both pipeline and backend must point to the **same** cluster — the pipeline is the only writer to `entities` and `entities_vectors`.

Stale documents are only cleaned up on full runs (all sources). After `MONGO_DELETE_AFTER_MISSED_FULL_RUNS` (default 2) consecutive full-run absences, a document is deleted if `MONGO_HARD_DELETE_STALE=true`, or marked inactive otherwise.

## Conventions & Patterns

### Unified schema (`EventoEstandar`)

Events and places share one schema; `entity_type` (`event` | `place`) differentiates them. Key identity fields:

- `entity_id`: MD5 hash of `source::url` — unique per entity
- `poi_id`: stable identity across runs (v1 algorithm)
- `fuente`: source identifier (`bnp` | `mali` | `joinnus` | `places`)
- `precio`: always one of `"Gratuito"` | `"Pago"` | `"Consultar"`
- `tipo`: normalized category — `concierto` · `exposición` · `teatro` · `cine` · `taller` · `conferencia` · `danza` · `familia` · `tour` · `gastronomía` · `deporte` · `cultural`

### Embedding text format

```
Evento: <titulo> | Tipo: <tipo> | Lugar: <lugar> | Ciudad: Lima, Perú |
Descripción: <descripcion> | Precio: <precio> | Tags: <tags>
```

For `entity_type=place`, the text also includes distrito, dirección, Google category, rating, and review summaries to improve disambiguation between similarly named venues.

### Google Places is static

`scrapers/scraper_google_places.py` reads from `input/google_places_payload.json` — it does **not** hit the Google API at runtime. Update that file manually when Places data needs refreshing.

### Partial runs must not clean MongoDB

The stale-document cleanup is intentionally skipped when `--sources` is used (partial run). Never add cleanup logic that runs unconditionally on partial runs.

### Run ID format

`YYYYMMdd_HHmmss` UTC — used as suffix for all snapshot files, lake paths, logs, and embedding artifacts.

### Environment config

Copy `cultural_pipeline/.env.example` to `cultural_pipeline/.env`. Key variables:

```
# Unified MongoDB cluster (pipeline + backend share the same cluster)
MONGO_URI_WEB=           # same URI as MONGO_URI_RECO
MONGO_DB_WEB=turislima
MONGO_COLL_WEB=entities
MONGO_URI_RECO=          # same URI as MONGO_URI_WEB
MONGO_DB_RECO=turislima
MONGO_COLL_RECO=entities_vectors

# Reco backend selector (atlas | cosmos)
RECO_BACKEND=atlas

# Lake storage (local | azure)
LAKE_BACKEND=local
# LAKE_LOCAL_ROOT=       # defaults to cultural_pipeline/data/
# AZURE_STORAGE_ACCOUNT_NAME=   # required when LAKE_BACKEND=azure

EMBEDDING_MODEL_NAME=paraphrase-multilingual-MiniLM-L12-v2
EMBEDDING_DIM=384
MONGO_HARD_DELETE_STALE=true
MONGO_DELETE_AFTER_MISSED_FULL_RUNS=2
GOOGLE_PLACES_STATIC_PATH=cultural_pipeline/input/google_places_payload.json

# Optional
GOOGLE_GEOCODING_API_KEY=   # enriches lat/lng for events without coordinates
DEEPSEEK_API_KEY=           # optional description enrichment
COSMOS_URI=                 # required only when RECO_BACKEND=cosmos
```
