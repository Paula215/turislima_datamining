# Project Guidelines

This repository contains several data-mining experiments. The production-oriented flow is in `cultural_pipeline/` (3 scrapers + normalization + embeddings + scheduled runs).

## Build And Run

- Install dependencies:
  - `cd cultural_pipeline`
  - `pip install --upgrade pip`
  - `pip install -r requirements.txt`
- Run full pipeline:
  - `python pipeline/pipeline.py`
- Run selected sources only:
  - `python pipeline/pipeline.py --sources bnp mali`
- Run without embeddings:
  - `python pipeline/pipeline.py --skip-embeddings`
- Dry run using existing standardized data:
  - `python pipeline/pipeline.py --dry-run`
- Scheduler options:
  - `python scheduler/scheduler.py` (daemon)
  - `python scheduler/scheduler.py --install-cron`
- There are currently no automated tests in this repo. Validate runs with `logs/run_*.log`, `logs/stats_*.json`, and `output/eventos_estandar.csv`.

## Architecture

- `cultural_pipeline/scrapers/`: source-specific extractors (`scraper_bnp.py`, `scraper_mali.py`, `scraper_joinnus.py`) returning raw `pandas.DataFrame`.
- `cultural_pipeline/pipeline/normalizer.py`: transforms raw rows into the unified event schema.
- `cultural_pipeline/pipeline/pipeline.py`: orchestrates scraping in parallel, normalization, output persistence, and optional embeddings.
- `cultural_pipeline/embeddings/embedder.py`: semantic vectors + optional FAISS index + search helper.
- `cultural_pipeline/scheduler/scheduler.py`: scheduled execution via daemon or crontab.

For full architecture and output details, see `cultural_pipeline/README.md`.

## Conventions

- Scrapers should expose `run() -> pd.DataFrame` and include `_source` and `_scraped_at` columns.
- Keep source-specific parsing inside each scraper; keep cross-source transformations in `normalizer.py`.
- Preserve the standardized output schema produced by `normalize_all` (event identity, temporal fields, source, city, semantic text).
- Keep CSV output encoding as `utf-8-sig` for spreadsheet compatibility; JSON/logs use `utf-8`.
- Use `datetime.utcnow()` for run IDs/timestamps, and document local-time implications explicitly when touching scheduler logic.
- In `pipeline.py`, scraping concurrency is intentionally conservative (`ThreadPoolExecutor(max_workers=2)`) to avoid overloading source sites.

## Pitfalls

- `scraper_mali.py` depends on Selenium + ChromeDriver; CI/servers must provide a compatible Chrome runtime.
- Embeddings default to `sentence-transformers`; OpenAI fallback needs `OPENAI_API_KEY` and optional `openai` dependency.
- FAISS is optional in `embedder.py`; missing `faiss` should not break the pipeline.
- `--dry-run` requires a pre-existing `output/eventos_estandar.csv`.
- Scheduled jobs use UTC references; keep comments and cron expressions aligned (e.g., 07:00 UTC = 02:00 Lima, UTC-5).

## Key References

- `cultural_pipeline/README.md`
- `cultural_pipeline/requirements.txt`
- `cultural_pipeline/pipeline/pipeline.py`
- `cultural_pipeline/pipeline/normalizer.py`
- `cultural_pipeline/embeddings/embedder.py`
- `cultural_pipeline/scheduler/scheduler.py`