"""Nivel 3: probar el trigger COMPLETO de Reclamos (reclamos_bronze_a_silver),
el que corre solo todos los dias a medianoche, sin conectarse a Azure para
nada (ver tests/_fakes_azure.py para como se arma el mock)."""
import json
from datetime import datetime, timezone
from io import BytesIO
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

import reclamos.bp_reclamos_silver as silver_mod
from tests._fakes_azure import ContenedorBronzeFalso, ContenedorSilverFalso, blob_service_client_falso


@pytest.fixture(autouse=True)
def _env_falso(monkeypatch):
    # El codigo real chequea que esta variable exista antes de conectarse;
    # como BlobServiceClient esta mockeado, el valor nunca se usa de verdad.
    monkeypatch.setenv("STORAGE_CONNECTION_STRING", "conexion-de-mentira")


def test_trigger_procesa_un_evento_nuevo_y_deja_todo_actualizado():
    creado_en = datetime(2026, 9, 1, 8, 0, tzinfo=timezone.utc)
    evento = json.dumps({
        "metadata": {"eventType": "com.citypass.reclamos.ReclamoCreado", "eventId": "evt-1"},
        "data": {"reclamoId": "R-900", "categoria": "BACHES", "createdAt": int(creado_en.timestamp() * 1000)},
    }).encode("utf-8")
    ultima_modificacion = datetime(2026, 9, 1, 8, 5, tzinfo=timezone.utc)

    bronze = ContenedorBronzeFalso({
        "Reclamos/ReclamoCreado_20260901_080000_abc.json": (evento, ultima_modificacion),
    })
    silver = ContenedorSilverFalso()  # primera corrida, sin checkpoint

    with patch.object(
        silver_mod.BlobServiceClient, "from_connection_string",
        return_value=blob_service_client_falso(bronze=bronze, silver=silver),
    ):
        silver_mod.reclamos_bronze_a_silver(MagicMock())

    # 1. Se "subio" (guardo en nuestro objeto falso) una tabla con el reclamo procesado.
    assert len(silver.tabla_blob.subidas) == 1
    df_resultante = pd.read_parquet(BytesIO(silver.tabla_blob.subidas[0]))
    assert len(df_resultante) == 1
    assert df_resultante.iloc[0]["reclamoId"] == "R-900"

    # 2. El checkpoint se actualizo con la fecha del blob que se proceso.
    assert len(silver.checkpoint_blob.subidas) == 1
    checkpoint_guardado = json.loads(silver.checkpoint_blob.subidas[0])
    assert checkpoint_guardado["ultima_corrida"] == ultima_modificacion.isoformat()


def test_trigger_no_escribe_nada_si_no_hay_blobs_nuevos_desde_el_checkpoint():
    checkpoint_previo = datetime(2026, 9, 5, 0, 0, tzinfo=timezone.utc)
    bronze = ContenedorBronzeFalso({
        # Este blob es ANTERIOR al checkpoint -> ya deberia estar procesado.
        "Reclamos/ReclamoCreado_viejo.json": (b"{}", datetime(2026, 9, 1, tzinfo=timezone.utc)),
    })
    silver = ContenedorSilverFalso(
        checkpoint_previo=json.dumps({"ultima_corrida": checkpoint_previo.isoformat()})
    )

    with patch.object(
        silver_mod.BlobServiceClient, "from_connection_string",
        return_value=blob_service_client_falso(bronze=bronze, silver=silver),
    ):
        silver_mod.reclamos_bronze_a_silver(MagicMock())

    # No hay nada nuevo que procesar -> no se debe haber subido nada a Azure
    # (evita escrituras innecesarias en cada corrida diaria).
    assert silver.tabla_blob.subidas == []
    assert silver.checkpoint_blob.subidas == []
