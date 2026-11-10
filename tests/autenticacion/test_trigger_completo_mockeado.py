"""Nivel 3: trigger completo de Autenticacion (autenticacion_bronze_a_silver).
Mismo patron de orquestacion que el resto -- confirma el cableado propio de
Autenticacion (prefijo, blob de silver, ingested_at = last_modified del blob
de bronze)."""
import json
from datetime import datetime, timezone
from io import BytesIO
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

import autenticacion.bp_autenticacion_silver as silver_mod
from tests._fakes_azure import ContenedorBronzeFalso, ContenedorSilverFalso, blob_service_client_falso


@pytest.fixture(autouse=True)
def _env_falso(monkeypatch):
    monkeypatch.setenv("STORAGE_CONNECTION_STRING", "conexion-de-mentira")


def test_trigger_procesa_un_login_nuevo():
    evento = json.dumps({
        "metadata": {"eventType": "com.citypass.auth.LoginExitoso"},
        "data": {
            "authEventId": "AE-900", "actorSub": "U-900", "occurredAt": 1793362500000,
            "username": "nicolas", "department": "reclamos", "clientId": "citypass-reclamos-web",
            "chainId": None, "ipAddress": "192.168.1.10", "userAgent": "Mozilla/5.0",
        },
    }).encode("utf-8")
    ultima_modificacion = datetime(2026, 9, 1, 8, 1, tzinfo=timezone.utc)

    bronze = ContenedorBronzeFalso({
        "LDAP/LoginExitoso/LoginExitoso_20260901_080000_abc.json": (evento, ultima_modificacion),
    })
    silver = ContenedorSilverFalso()

    with patch.object(
        silver_mod.BlobServiceClient, "from_connection_string",
        return_value=blob_service_client_falso(bronze=bronze, silver=silver),
    ):
        silver_mod.autenticacion_bronze_a_silver(MagicMock())

    assert len(silver.tabla_blob.subidas) == 1
    df_resultante = pd.read_parquet(BytesIO(silver.tabla_blob.subidas[0]))
    assert len(df_resultante) == 1
    assert df_resultante.iloc[0]["auth_event_id"] == "AE-900"
    assert df_resultante.iloc[0]["ingested_at"] == pd.Timestamp(ultima_modificacion)

    checkpoint_guardado = json.loads(silver.checkpoint_blob.subidas[0])
    assert checkpoint_guardado["ultima_corrida"] == ultima_modificacion.isoformat()
