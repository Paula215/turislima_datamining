"""LocalStore — filesystem-backed lake storage for development."""

from __future__ import annotations

import os
from pathlib import Path

from ._protocol import BaseLakeStore


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
