"""Nivel 3 de Gold (Movilidad): confirma lectura de Silver mockeada, el
bucketizado de duracion, y snapshot los domingos."""
from datetime import datetime, timezone
from io import BytesIO
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

import movilidad.bp_movilidad_gold as gold_mod
from tests._fakes_azure import ContenedorGoldFalso, ContenedorSilverFalso, blob_service_client_falso


@pytest.fixture(autouse=True)
def _env_falso(monkeypatch):
    monkeypatch.setenv("STORAGE_CONNECTION_STRING", "conexion-de-mentira")


def _tabla_silver_de_prueba() -> bytes:
    inicio = datetime(2026, 9, 1, 8, 0, tzinfo=timezone.utc)
    filas = [{"viajeId": "V-001", "estacionInicio": "liniers-02", "horaInicio": inicio, "duracion_minutos": 20.0}]
    df = pd.DataFrame(filas)
    df["horaInicio"] = pd.to_datetime(df["horaInicio"], utc=True)
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    return buffer.getvalue()


def test_trigger_gold_calcula_resumen_y_guarda_snapshot_los_domingos():
    silver = ContenedorSilverFalso(tabla_previa=_tabla_silver_de_prueba())
    gold = ContenedorGoldFalso()
    fecha_domingo = datetime(2026, 9, 20, 3, 0, tzinfo=timezone.utc)

    with patch.object(
        gold_mod.BlobServiceClient, "from_connection_string",
        return_value=blob_service_client_falso(silver=silver, gold=gold),
    ), patch.object(gold_mod, "datetime") as datetime_mock:
        datetime_mock.now.return_value = fecha_domingo
        gold_mod.movilidad_silver_a_gold(MagicMock())

    acumulado = gold._blobs["Movilidad Urbana/viajes_resumen.parquet"]
    assert len(acumulado.subidas) == 1
    df_resumen = pd.read_parquet(BytesIO(acumulado.subidas[0]))
    assert df_resumen.iloc[0]["cantidadViajes"] == 1
    assert df_resumen.iloc[0]["duracionViaje"] == "15-30min"

    anio, semana, _ = fecha_domingo.isocalendar()
    nombre_snapshot = f"Movilidad Urbana/viajes_resumen_{semana:02d}_{anio}.parquet"
    assert nombre_snapshot in gold._blobs
    assert len(gold._blobs[nombre_snapshot].subidas) == 1
