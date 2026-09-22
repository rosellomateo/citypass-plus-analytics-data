"""Nivel 3: trigger completo de Movilidad (movilidad_bronze_a_silver).
Mismo patron de orquestacion que Reclamos/Emergencias -- este test confirma
el cableado propio de Movilidad (prefijo, blob de silver, dispatch al
ViajeIniciado)."""
import json
from datetime import datetime, timezone
from io import BytesIO
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

import movilidad.bp_movilidad_silver as silver_mod
from tests._fakes_azure import ContenedorBronzeFalso, ContenedorSilverFalso, blob_service_client_falso


@pytest.fixture(autouse=True)
def _env_falso(monkeypatch):
    monkeypatch.setenv("STORAGE_CONNECTION_STRING", "conexion-de-mentira")


def test_trigger_procesa_un_viaje_iniciado_nuevo():
    inicio = datetime(2026, 9, 1, 8, 0, tzinfo=timezone.utc)
    evento = json.dumps({
        "metadata": {"eventType": "com.citypass.movilidad.ViajeIniciado"},
        "data": {"id": "V-900", "estacion": "liniers-02", "horaInicio": inicio.isoformat()},
    }).encode("utf-8")
    ultima_modificacion = datetime(2026, 9, 1, 8, 1, tzinfo=timezone.utc)

    bronze = ContenedorBronzeFalso({
        "Movilidad Urbana/ViajeIniciado_20260901_080000_abc.json": (evento, ultima_modificacion),
    })
    silver = ContenedorSilverFalso()

    with patch.object(
        silver_mod.BlobServiceClient, "from_connection_string",
        return_value=blob_service_client_falso(bronze=bronze, silver=silver),
    ):
        silver_mod.movilidad_bronze_a_silver(MagicMock())

    assert len(silver.tabla_blob.subidas) == 1
    df_resultante = pd.read_parquet(BytesIO(silver.tabla_blob.subidas[0]))
    assert len(df_resultante) == 1
    assert df_resultante.iloc[0]["viajeId"] == "V-900"
    assert df_resultante.iloc[0]["en_curso"] == True  # noqa: E712 -- todavia no tiene ViajeTerminado

    checkpoint_guardado = json.loads(silver.checkpoint_blob.subidas[0])
    assert checkpoint_guardado["ultima_corrida"] == ultima_modificacion.isoformat()
