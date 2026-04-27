"""
Stages del pipeline medallón.

Cada stage es un módulo independiente con una función `run(...)` que
devuelve los artefactos necesarios para el siguiente. El orquestador
(`pipeline/pipeline.py`) decide qué stages correr según `--stage`.

Stages:
  - bronze: scrapers → dump_to_bronze + raws auditables + BronzeManifest
  - silver: normalizer + geocoding → Silver Parquet + legacy CSV/JSON + SilverManifest
  - gold:   enrichment + embeddings → Gold catalog/vectors Parquet + sinks + GoldManifest
"""

from . import bronze, silver, gold

__all__ = ["bronze", "silver", "gold"]
