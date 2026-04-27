"""AzureStore — ADLS Gen2 backed lake storage."""

from __future__ import annotations

import os

from ._protocol import BaseLakeStore


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
