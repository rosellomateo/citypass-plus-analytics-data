"""Nivel 3 de Gold (Autenticacion): confirma que el trigger lee Silver
(mockeado), calcula el resumen, lo sube, y que -si el dia es domingo-
ADEMAS guarda el snapshot semanal con el nombre correcto."""
from datetime import datetime, timezone
from io import BytesIO
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

import autenticacion.bp_autenticacion_gold as gold_mod
from tests._fakes_azure import ContenedorGoldFalso, ContenedorSilverFalso, blob_service_client_falso


@pytest.fixture(autouse=True)
def _env_falso(monkeypatch):
    monkeypatch.setenv("STORAGE_CONNECTION_STRING", "conexion-de-mentira")


def _tabla_silver_de_prueba() -> bytes:
    ocurrido = datetime(2026, 9, 1, tzinfo=timezone.utc)
    filas = [
        {
            "auth_event_id": "AE-001", "department": "reclamos", "username": "nicolas",
            "event_type": "com.citypass.auth.LoginExitoso", "actor_sub": "U-001",
            "event_time_utc": ocurrido, "ingested_at": ocurrido,
        },
        {
            "auth_event_id": "AE-002", "department": "reclamos", "username": "nicolas",
            "event_type": "com.citypass.auth.LoginExitoso", "actor_sub": "U-001",
            "event_time_utc": ocurrido, "ingested_at": ocurrido,
        },
    ]
    df = pd.DataFrame(filas)
    for col in ("event_time_utc", "ingested_at"):
        df[col] = pd.to_datetime(df[col], utc=True)
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
        gold_mod.autenticacion_silver_a_gold(MagicMock())

    acumulado = gold._blobs["LDAP/autenticacion_resumen.parquet"]
    assert len(acumulado.subidas) == 1
    df_resumen = pd.read_parquet(BytesIO(acumulado.subidas[0]))
    assert df_resumen.iloc[0]["cantidadEventos"] == 2

    anio, semana, _ = fecha_domingo.isocalendar()
    nombre_snapshot = f"LDAP/autenticacion_resumen_{semana:02d}_{anio}.parquet"
    assert nombre_snapshot in gold._blobs
    assert len(gold._blobs[nombre_snapshot].subidas) == 1
