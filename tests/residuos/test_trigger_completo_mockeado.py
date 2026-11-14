"""Nivel 3: trigger completo de Residuos (residuos_bronze_a_silver). Mismo
patron de orquestacion -- este test confirma el cableado propio de Residuos
(prefijo, blob de silver, dispatch a IntervencionProgramada)."""
import json
from datetime import datetime, timezone
from io import BytesIO
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

import residuos.bp_residuos_silver as silver_mod
from tests._fakes_azure import ContenedorBronzeFalso, ContenedorSilverFalso, blob_service_client_falso


@pytest.fixture(autouse=True)
def _env_falso(monkeypatch):
    monkeypatch.setenv("STORAGE_CONNECTION_STRING", "conexion-de-mentira")


def test_trigger_procesa_una_intervencion_nueva():
    programada_en = datetime(2026, 9, 1, 8, 0, tzinfo=timezone.utc)
    evento = json.dumps({
        "metadata": {"eventType": "com.citypass.residuos.IntervencionProgramada"},
        "data": {
            "intervencionId": "I-900", "contenedorId": "cont-900",
            "latitud": -34.6, "longitud": -58.4,
            "programadaEn": int(programada_en.timestamp() * 1000),
            "occurredAt": int(programada_en.timestamp() * 1000),
            "causaId": None, "causaTipo": None, "camionId": None,
        },
    }).encode("utf-8")
    ultima_modificacion = datetime(2026, 9, 1, 8, 1, tzinfo=timezone.utc)

    bronze = ContenedorBronzeFalso({
        "Gestion de Residuos Inteligente/IntervencionProgramada/IntervencionProgramada_20260901_080000_abc.json": (evento, ultima_modificacion),
    })
    silver = ContenedorSilverFalso()

    with patch.object(
        silver_mod.BlobServiceClient, "from_connection_string",
        return_value=blob_service_client_falso(bronze=bronze, silver=silver),
    ):
        silver_mod.residuos_bronze_a_silver(MagicMock())

    assert len(silver.tabla_blob.subidas) == 1
    df_resultante = pd.read_parquet(BytesIO(silver.tabla_blob.subidas[0]))
    assert len(df_resultante) == 1
    assert df_resultante.iloc[0]["intervencionId"] == "I-900"
    assert df_resultante.iloc[0]["estado_actual"] == "PROGRAMADA"

    checkpoint_guardado = json.loads(silver.checkpoint_blob.subidas[0])
    assert checkpoint_guardado["ultima_corrida"] == ultima_modificacion.isoformat()
