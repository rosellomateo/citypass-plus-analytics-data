"""Nivel 3: trigger completo de Emergencias (emergencias_bronze_a_silver).
La orquestacion es identica a la de Reclamos (mismo patron de checkpoint) --
lo que este test confirma es que el CABLEADO es correcto para este dominio
puntual: el prefijo de bronze, el nombre del blob de silver, y el dispatch
al evento correcto."""
import json
from datetime import datetime, timezone
from io import BytesIO
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

import emergencias.bp_emergencias_silver as silver_mod
from tests._fakes_azure import ContenedorBronzeFalso, ContenedorSilverFalso, blob_service_client_falso


@pytest.fixture(autouse=True)
def _env_falso(monkeypatch):
    monkeypatch.setenv("STORAGE_CONNECTION_STRING", "conexion-de-mentira")


def test_trigger_procesa_una_emergencia_nueva():
    creado_en = datetime(2026, 9, 1, 8, 0, tzinfo=timezone.utc)
    evento = json.dumps({
        "metadata": {"eventType": "com.citypass.emergencias.EmergenciaCreada"},
        "data": {"emergenciaId": "E-900", "prioridad": "ALTA", "createdAt": creado_en.isoformat()},
    }).encode("utf-8")
    ultima_modificacion = datetime(2026, 9, 1, 8, 5, tzinfo=timezone.utc)

    bronze = ContenedorBronzeFalso({
        "Emergencias y Seguridad/EmergenciaCreada_20260901_080000_abc.json": (evento, ultima_modificacion),
    })
    silver = ContenedorSilverFalso()

    with patch.object(
        silver_mod.BlobServiceClient, "from_connection_string",
        return_value=blob_service_client_falso(bronze=bronze, silver=silver),
    ):
        silver_mod.emergencias_bronze_a_silver(MagicMock())

    assert len(silver.tabla_blob.subidas) == 1
    df_resultante = pd.read_parquet(BytesIO(silver.tabla_blob.subidas[0]))
    assert len(df_resultante) == 1
    assert df_resultante.iloc[0]["emergenciaId"] == "E-900"
    assert df_resultante.iloc[0]["estado_actual"] == "PENDIENTE"

    checkpoint_guardado = json.loads(silver.checkpoint_blob.subidas[0])
    assert checkpoint_guardado["ultima_corrida"] == ultima_modificacion.isoformat()
