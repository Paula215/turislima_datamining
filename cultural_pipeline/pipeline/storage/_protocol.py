"""
BaseLakeStore ABC and get_store() factory.

Subclasses implement only write_bytes / read_bytes / exists.
All medallion-aware methods (write_bronze, write_silver, etc.)
are defined once here and work identically on every backend.
"""

from __future__ import annotations

import abc
import io
import json
import os
from typing import Union

import pyarrow as pa
import pyarrow.parquet as pq

from contracts import (  # type: ignore[import-not-found]
    BronzeManifest,
    GoldManifest,
    SilverManifest,
    bronze_path,
    gold_catalog_path,
    gold_vectors_path,
    manifest_path,
    silver_path,
)
from contracts.manifests import to_json  # type: ignore[import-not-found]


class BaseLakeStore(abc.ABC):
    """Backend-agnostic lake I/O.  Subclass and implement the three abstract methods."""

    @abc.abstractmethod
    def write_bytes(self, path: str, data: bytes) -> None: ...

    @abc.abstractmethod
    def read_bytes(self, path: str) -> bytes: ...

    @abc.abstractmethod
    def exists(self, path: str) -> bool: ...

    # ------------------------------------------------------------------
    # Bronze
    # ------------------------------------------------------------------

    def write_bronze(
        self,
        source: str,
        run_id: str,
        payload: Union[bytes, str],
        filename: str = "payload.json",
    ) -> str:
        p = bronze_path(source, run_id, filename)
        raw = payload if isinstance(payload, bytes) else payload.encode("utf-8")
        self.write_bytes(p, raw)
        return p

    def read_bronze(
        self,
        source: str,
        run_id: str,
        filename: str = "payload.json",
    ) -> bytes:
        return self.read_bytes(bronze_path(source, run_id, filename))

    # ------------------------------------------------------------------
    # Parquet helpers
    # ------------------------------------------------------------------

    def write_parquet(self, path: str, table: pa.Table) -> str:
        buf = io.BytesIO()
        pq.write_table(table, buf)
        self.write_bytes(path, buf.getvalue())
        return path

    def read_parquet(self, path: str) -> pa.Table:
        return pq.read_table(io.BytesIO(self.read_bytes(path)))

    # ------------------------------------------------------------------
    # Silver
    # ------------------------------------------------------------------

    def write_silver(self, run_id: str, table: pa.Table, part: int = 0) -> str:
        return self.write_parquet(silver_path(run_id, part), table)

    def read_silver(self, run_id: str, part: int = 0) -> pa.Table:
        return self.read_parquet(silver_path(run_id, part))

    # ------------------------------------------------------------------
    # Gold
    # ------------------------------------------------------------------

    def write_gold_catalog(self, run_id: str, table: pa.Table, part: int = 0) -> str:
        return self.write_parquet(gold_catalog_path(run_id, part), table)

    def write_gold_vectors(self, run_id: str, table: pa.Table) -> str:
        return self.write_parquet(gold_vectors_path(run_id), table)

    # ------------------------------------------------------------------
    # Manifests
    # ------------------------------------------------------------------

    def write_manifest(
        self,
        layer: str,
        run_id: str,
        manifest: Union[BronzeManifest, SilverManifest, GoldManifest],
    ) -> str:
        data = to_json(manifest).encode("utf-8")
        p = manifest_path(layer, run_id)
        self.write_bytes(p, data)
        self.write_bytes(manifest_path(layer, None), data)
        return p

    def get_latest_run_id(self, layer: str) -> str | None:
        latest = manifest_path(layer, None)
        if not self.exists(latest):
            return None
        raw = json.loads(self.read_bytes(latest))
        return raw.get("run_id")


def get_store() -> BaseLakeStore:
    """Return a LakeStore for the configured backend (LAKE_BACKEND env var)."""
    backend = os.getenv("LAKE_BACKEND", "local").lower()
    if backend == "local":
        from ._local import LocalStore

        return LocalStore()
    if backend == "azure":
        from ._azure import AzureStore

        return AzureStore()
    raise ValueError(f"Unknown LAKE_BACKEND: {backend!r}. Expected 'local' or 'azure'.")
