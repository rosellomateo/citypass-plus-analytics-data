"""Nivel 3 de Gold (Movilidad): confirma que el trigger lee las DOS tablas
de silver (viajes + estaciones_afectadas) y escribe el resumen combinado
por causaTipo, con snapshot los domingos."""
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


def _tabla_viajes() -> bytes:
    df = pd.DataFrame([
        {"viajeId": "V-001", "causaTipo": "INCIDENTE_VIAL"},
        {"viajeId": "V-002", "causaTipo": None},
    ])
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    return buffer.getvalue()


def _tabla_estaciones() -> bytes:
    momento = datetime(2026, 11, 12, tzinfo=timezone.utc)
    df = pd.DataFrame([
        {
            "afectacionId": "AF-1", "estacionId": "EST-01", "causaTipo": "EVENTO_CLIMATICO",
            "tipoAfectacion": "BLOQUEADA", "afectacionDesde": momento, "occurredAt": momento,
            "restaurada": True,
        },
    ])
    df["afectacionDesde"] = pd.to_datetime(df["afectacionDesde"], utc=True)
    df["occurredAt"] = pd.to_datetime(df["occurredAt"], utc=True)
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    return buffer.getvalue()


def test_trigger_gold_combina_las_dos_tablas_y_guarda_snapshot_los_domingos():
    silver = ContenedorSilverFalso(tablas={
        "Movilidad Urbana/viajes.parquet": _tabla_viajes(),
        "Movilidad Urbana/estaciones_afectadas.parquet": _tabla_estaciones(),
    })
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

    fila_clima = df_resumen[df_resumen["causaTipo"] == "EVENTO_CLIMATICO"].iloc[0]
    assert fila_clima["cantidadBloqueadas"] == 1
    assert fila_clima["cantidadViajesInterrumpidos"] == 0

    fila_incidente = df_resumen[df_resumen["causaTipo"] == "INCIDENTE_VIAL"].iloc[0]
    assert fila_incidente["cantidadViajesInterrumpidos"] == 1
    assert fila_incidente["cantidadAfectaciones"] == 0

    # V-002 (causaTipo=None) cae en su propia fila NORMAL.
    fila_normal = df_resumen[df_resumen["causaTipo"] == "NORMAL"].iloc[0]
    assert fila_normal["cantidadViajesInterrumpidos"] == 1
    assert fila_normal["cantidadAfectaciones"] == 0

    anio, semana, _ = fecha_domingo.isocalendar()
    nombre_snapshot = f"Movilidad Urbana/viajes_resumen_{semana:02d}_{anio}.parquet"
    assert nombre_snapshot in gold._blobs
    assert len(gold._blobs[nombre_snapshot].subidas) == 1
