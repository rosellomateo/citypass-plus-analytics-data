"""Helper compartido por los scripts de scripts/ para conectarse al storage
usando la misma STORAGE_CONNECTION_STRING que usa la function app."""
import json
import os
from pathlib import Path

from azure.storage.blob import BlobServiceClient

REPO_ROOT = Path(__file__).resolve().parent.parent


def connection_string() -> str:
    if "STORAGE_CONNECTION_STRING" in os.environ:
        return os.environ["STORAGE_CONNECTION_STRING"]

    local_settings = REPO_ROOT / "local.settings.json"
    valores = json.loads(local_settings.read_text(encoding="utf-8"))["Values"]
    return valores["STORAGE_CONNECTION_STRING"]


def blob_service_client() -> BlobServiceClient:
    return BlobServiceClient.from_connection_string(connection_string())
