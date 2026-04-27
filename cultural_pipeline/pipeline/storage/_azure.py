"""AzureStore — ADLS Gen2 backed lake storage."""

from __future__ import annotations

import os
import re

from ._protocol import BaseLakeStore


_RUN_ID_PATTERN = re.compile(r"run_id=(?P<run_id>[0-9_]+)")


class AzureStore(BaseLakeStore):
    """Reads/writes lake artefacts in an Azure Data Lake Storage Gen2 container."""

    def __init__(self) -> None:
        from azure.identity import DefaultAzureCredential
        from azure.storage.filedatalake import DataLakeServiceClient

        account = os.environ["AZURE_STORAGE_ACCOUNT_NAME"]
        filesystem = os.getenv("ADLS_FILESYSTEM", "lake")
        url = f"https://{account}.dfs.core.windows.net"
        service = DataLakeServiceClient(url, credential=DefaultAzureCredential())
        self._fs = service.get_file_system_client(filesystem)

    def write_bytes(self, path: str, data: bytes) -> None:
        self._fs.get_file_client(path).upload_data(data, overwrite=True)

    def read_bytes(self, path: str) -> bytes:
        return self._fs.get_file_client(path).download_file().readall()

    def exists(self, path: str) -> bool:
        from azure.core.exceptions import ResourceNotFoundError

        try:
            self._fs.get_file_client(path).get_file_properties()
            return True
        except ResourceNotFoundError:
            return False

    def list_run_ids(self, layer: str, dataset: str | None = None) -> list[str]:
        if layer not in ("bronze", "silver", "gold"):
            raise ValueError(f"Unknown layer: {layer!r}")
        prefix = layer if not dataset else f"{layer}/{dataset}"
        run_ids: set[str] = set()
        for path in self._fs.get_paths(path=prefix, recursive=True):
            m = _RUN_ID_PATTERN.search(path.name)
            if m:
                run_ids.add(m.group("run_id"))
        return sorted(run_ids)
