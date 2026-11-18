"""Nivel 3: trigger completo de Movilidad (movilidad_bronze_a_silver), el
que procesa viajes (ViajeFinalizado). El de estaciones afectadas
(procesar_estaciones) corre dentro del mismo trigger;
ver los tests del final del archivo."""
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


def test_trigger_procesa_un_viaje_finalizado_nuevo():
    inicio = datetime(2026, 9, 1, 8, 0, tzinfo=timezone.utc)
    fin = datetime(2026, 9, 1, 8, 25, tzinfo=timezone.utc)
    evento = json.dumps({
        "metadata": {"eventType": "com.citypass.movilidad.ViajeFinalizado"},
        "data": {
            "viajeId": "V-900", "correlationId": "viaje-V-900",
            "estacionInicio": "EST-007", "latitudInicio": -34.6066, "longitudInicio": -58.4357,
            "horaInicio": inicio.isoformat(),
            "estacionFin": "EST-010", "latitudFin": -34.5622, "longitudFin": -58.4522,
            "horaFinalizacion": fin.isoformat(),
            "causaId": None, "causaTipo": None,
            "createdAt": fin.isoformat(),
        },
    }).encode("utf-8")
    ultima_modificacion = datetime(2026, 9, 1, 8, 26, tzinfo=timezone.utc)

    bronze = ContenedorBronzeFalso({
        "Movilidad Urbana/ViajeFinalizado_20260901_082500_abc.json": (evento, ultima_modificacion),
    })
    silver = ContenedorSilverFalso()

    # Este test es solo de viajes: estaciones tiene sus propios tests abajo.
    with patch.object(
        silver_mod.BlobServiceClient, "from_connection_string",
        return_value=blob_service_client_falso(bronze=bronze, silver=silver),
    ), patch.object(silver_mod, "procesar_estaciones"):
        silver_mod.movilidad_bronze_a_silver(MagicMock())

    assert len(silver.tabla_blob.subidas) == 1
    df_resultante = pd.read_parquet(BytesIO(silver.tabla_blob.subidas[0]))
    assert len(df_resultante) == 1
    assert df_resultante.iloc[0]["viajeId"] == "V-900"
    assert df_resultante.iloc[0]["duracion_minutos"] == 25.0

    checkpoint_guardado = json.loads(silver.checkpoint_blob.subidas[0])
    assert checkpoint_guardado["ultima_corrida"] == ultima_modificacion.isoformat()


def test_trigger_tambien_procesa_estaciones_afectadas_en_la_misma_corrida():
    """El timer propio de estaciones no se disparaba en Azure: ahora
    movilidad_bronze_a_silver arma las dos tablas en cada corrida."""
    momento = datetime(2026, 9, 1, 9, 0, tzinfo=timezone.utc)
    afectada = json.dumps({
        "metadata": {"eventType": "com.citypass.movilidad.EstacionUrbanaAfectada"},
        "data": {
            "afectacionId": "AF-1", "estacionId": "EST-1", "latitud": -34.6, "longitud": -58.4,
            "tipoAfectacion": "BLOQUEADA", "afectacionDesde": momento.isoformat(),
            "afectacionHastaEstimada": None, "causaId": "AL-1", "causaTipo": "EVENTO_CLIMATICO",
            "moduloOrigen": "espacios", "correlationId": "AL-1", "occurredAt": momento.isoformat(),
        },
    }).encode("utf-8")
    bronze = ContenedorBronzeFalso({
        "Movilidad Urbana/EstacionUrbanaAfectada/x.json": (afectada, momento),
    })
    silver = ContenedorSilverFalso(tablas={
        silver_mod.TABLA_VIAJES_BLOB: None,
        silver_mod.TABLA_ESTACIONES_BLOB: None,
        silver_mod.CHECKPOINT_ESTACIONES_BLOB: None,
    })

    with patch.object(
        silver_mod.BlobServiceClient, "from_connection_string",
        return_value=blob_service_client_falso(bronze=bronze, silver=silver),
    ):
        silver_mod.movilidad_bronze_a_silver(MagicMock())

    estaciones = silver.tablas_por_nombre[silver_mod.TABLA_ESTACIONES_BLOB]
    assert len(estaciones.subidas) == 1
    df = pd.read_parquet(BytesIO(estaciones.subidas[0]))
    assert df.iloc[0]["afectacionId"] == "AF-1"
    assert df.iloc[0]["causaTipo"] == "EVENTO_CLIMATICO"
    checkpoint = json.loads(silver.tablas_por_nombre[silver_mod.CHECKPOINT_ESTACIONES_BLOB].subidas[0])
    assert checkpoint["ultima_corrida"] == momento.isoformat()


def test_si_falla_estaciones_igual_se_procesan_los_viajes():
    with patch.object(silver_mod, "procesar_estaciones", side_effect=RuntimeError("boom")), \
         patch.object(silver_mod, "procesar_viajes") as viajes:
        silver_mod.movilidad_bronze_a_silver(MagicMock())
    viajes.assert_called_once()


def test_si_falla_estaciones_el_error_queda_en_un_blob_de_silver():
    """App Insights esta en otra suscripcion: el traceback se guarda en silver."""
    silver = ContenedorSilverFalso(tablas={"Movilidad Urbana/_ultimo_error_estaciones.txt": None})
    with patch.object(silver_mod, "procesar_estaciones", side_effect=RuntimeError("boom")), \
         patch.object(silver_mod, "procesar_viajes"), \
         patch.object(silver_mod.BlobServiceClient, "from_connection_string",
                      return_value=blob_service_client_falso(bronze=ContenedorBronzeFalso({}), silver=silver)):
        silver_mod.movilidad_bronze_a_silver(MagicMock())

    error = silver.tablas_por_nombre["Movilidad Urbana/_ultimo_error_estaciones.txt"].subidas
    assert len(error) == 1
    assert b"RuntimeError: boom" in error[0]
