"""LocalStore — filesystem-backed lake storage for development."""

from __future__ import annotations

import os
import re
from pathlib import Path

from ._protocol import BaseLakeStore


_RUN_ID_DIR = re.compile(r"^run_id=(?P<run_id>[0-9_]+)$")


class LocalStore(BaseLakeStore):
    """Reads/writes lake artefacts under a local root directory."""

    def __init__(self, root: str | Path | None = None):
        if root is None:
            root = os.getenv("LAKE_LOCAL_ROOT", "")
        if not root:
            root = Path(__file__).resolve().parent.parent.parent / "data"
        self._root = Path(root)

    @property
    def root(self) -> Path:
        return self._root

    def write_bytes(self, path: str, data: bytes) -> None:
        full = self._root / path
        full.parent.mkdir(parents=True, exist_ok=True)
        full.write_bytes(data)

    def read_bytes(self, path: str) -> bytes:
        return (self._root / path).read_bytes()

    def exists(self, path: str) -> bool:
        return (self._root / path).exists()

    def list_run_ids(self, layer: str, dataset: str | None = None) -> list[str]:
        if layer not in ("bronze", "silver", "gold"):
            raise ValueError(f"Unknown layer: {layer!r}")
        base = self._root / layer
        if dataset:
            base = base / dataset
        if not base.exists():
            return []
        run_ids: set[str] = set()
        for child in base.rglob("run_id=*"):
            if not child.is_dir():
                continue
            m = _RUN_ID_DIR.match(child.name)
            if m:
                run_ids.add(m.group("run_id"))
        return sorted(run_ids)
