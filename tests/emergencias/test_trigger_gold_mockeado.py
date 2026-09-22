"""Nivel 3 de Gold (Emergencias): mismo mecanismo que Reclamos -- confirma
lectura de Silver mockeada, calculo del resumen, y snapshot los domingos."""
from datetime import datetime, timedelta, timezone
from io import BytesIO
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

import emergencias.bp_emergencias_gold as gold_mod
from tests._fakes_azure import ContenedorGoldFalso, ContenedorSilverFalso, blob_service_client_falso


@pytest.fixture(autouse=True)
def _env_falso(monkeypatch):
    monkeypatch.setenv("STORAGE_CONNECTION_STRING", "conexion-de-mentira")


def _tabla_silver_de_prueba() -> bytes:
    creada = datetime(2026, 9, 1, tzinfo=timezone.utc)
    filas = [{
        "emergenciaId": "E-001", "estado_actual": "DESPACHADA", "prioridad": "ALTA",
        "fecha_creada": creada, "fecha_despachada": creada + timedelta(minutes=15), "fecha_en_lugar": pd.NaT,
    }]
    df = pd.DataFrame(filas)
    for col in ("fecha_creada", "fecha_despachada", "fecha_en_lugar"):
        df[col] = pd.to_datetime(df[col], utc=True)
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
        gold_mod.emergencias_silver_a_gold(MagicMock())

    acumulado = gold._blobs["Emergencias y Seguridad/emergencias_resumen.parquet"]
    assert len(acumulado.subidas) == 1
    df_resumen = pd.read_parquet(BytesIO(acumulado.subidas[0]))
    assert df_resumen.iloc[0]["cantidadEmergencias"] == 1
    assert df_resumen.iloc[0]["tiempoPromRespuestaDespacho"] == 15.0

    anio, semana, _ = fecha_domingo.isocalendar()
    nombre_snapshot = f"Emergencias y Seguridad/emergencias_resumen_{semana:02d}_{anio}.parquet"
    assert nombre_snapshot in gold._blobs
    assert len(gold._blobs[nombre_snapshot].subidas) == 1
