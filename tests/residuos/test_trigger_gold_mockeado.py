"""Nivel 3 de Gold (Residuos): confirma que el trigger lee Silver
(mockeado), calcula el resumen, lo sube, y que -si el dia es domingo-
ADEMAS guarda el snapshot semanal con el nombre correcto."""
from datetime import datetime, timezone
from io import BytesIO
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

import residuos.bp_residuos_gold as gold_mod
from tests._fakes_azure import ContenedorGoldFalso, ContenedorSilverFalso, blob_service_client_falso


@pytest.fixture(autouse=True)
def _env_falso(monkeypatch):
    monkeypatch.setenv("STORAGE_CONNECTION_STRING", "conexion-de-mentira")


def _tabla_silver_de_prueba() -> bytes:
    filas = [
        {"intervencionId": "I-001", "estado_actual": "AFECTADA", "causaTipo": "EVENTO_PUBLICO", "afectada": True},
        {"intervencionId": "I-002", "estado_actual": "AFECTADA", "causaTipo": "EVENTO_PUBLICO", "afectada": True},
    ]
    df = pd.DataFrame(filas)
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    return buffer.getvalue()


def test_trigger_gold_calcula_resumen_y_guarda_snapshot_los_domingos():
    silver = ContenedorSilverFalso(tabla_previa=_tabla_silver_de_prueba())
    gold = ContenedorGoldFalso()
    fecha_domingo = datetime(2026, 9, 20, 3, 0, tzinfo=timezone.utc)  # confirmado que es domingo

    with patch.object(
        gold_mod.BlobServiceClient, "from_connection_string",
        return_value=blob_service_client_falso(silver=silver, gold=gold),
    ), patch.object(gold_mod, "datetime") as datetime_mock:
        datetime_mock.now.return_value = fecha_domingo
        gold_mod.residuos_silver_a_gold(MagicMock())

    acumulado = gold._blobs["Gestion de Residuos Inteligente/intervenciones_resumen.parquet"]
    assert len(acumulado.subidas) == 1
    df_resumen = pd.read_parquet(BytesIO(acumulado.subidas[0]))
    assert df_resumen.iloc[0]["cantidadIntervenciones"] == 2

    anio, semana, _ = fecha_domingo.isocalendar()
    nombre_snapshot = f"Gestion de Residuos Inteligente/intervenciones_resumen_{semana:02d}_{anio}.parquet"
    assert nombre_snapshot in gold._blobs
    assert len(gold._blobs[nombre_snapshot].subidas) == 1
