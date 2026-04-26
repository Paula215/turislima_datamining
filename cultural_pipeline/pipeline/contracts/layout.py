"""
Convenciones de paths en el lake (Bronze/Silver/Gold).

El layout es agnóstico al backend: las mismas funciones devuelven rutas
relativas que se interpretan contra `./data/` (backend local) o contra
el container `lake/` en ADLS Gen2.
"""

from __future__ import annotations

from dataclasses import dataclass


SOURCES = ("bnp", "mali", "joinnus", "places")


@dataclass(frozen=True)
class LakeLayout:
    """Constantes del layout. Inmutable para evitar drift."""

    bronze_root: str = "bronze"
    silver_root: str = "silver"
    gold_root: str = "gold"
    silver_dataset: str = "eventos_estandar"
    gold_catalog_dataset: str = "catalog_eventos"
    gold_vectors_dataset: str = "vectors"
    latest_manifest: str = "_latest.json"


LAYOUT = LakeLayout()


def bronze_path(source: str, run_id: str, filename: str = "payload") -> str:
    if source not in SOURCES:
        raise ValueError(f"Unknown source: {source!r}. Expected one of {SOURCES}.")
    return f"{LAYOUT.bronze_root}/source={source}/run_id={run_id}/{filename}"


def silver_path(run_id: str, part: int = 0) -> str:
    return (
        f"{LAYOUT.silver_root}/{LAYOUT.silver_dataset}/"
        f"run_id={run_id}/part-{part:04d}.parquet"
    )


def gold_catalog_path(run_id: str, part: int = 0) -> str:
    return (
        f"{LAYOUT.gold_root}/{LAYOUT.gold_catalog_dataset}/"
        f"run_id={run_id}/part-{part:04d}.parquet"
    )


def gold_vectors_path(run_id: str) -> str:
    return (
        f"{LAYOUT.gold_root}/{LAYOUT.gold_vectors_dataset}/"
        f"run_id={run_id}/vectors.parquet"
    )


def manifest_path(layer: str, run_id: str | None = None) -> str:
    """
    Path al manifest de un run o al puntero `_latest.json` de la capa.

    layer ∈ {"bronze", "silver", "gold"}.
    Si run_id es None, devuelve el path del puntero `_latest.json`.
    """
    if layer not in ("bronze", "silver", "gold"):
        raise ValueError(f"Unknown layer: {layer!r}")
    root = getattr(LAYOUT, f"{layer}_root")
    if run_id is None:
        return f"{root}/{LAYOUT.latest_manifest}"
    return f"{root}/_manifest_{run_id}.json"
