"""Nivel 3 de Gold (Reclamos): confirma que el trigger lee Silver (mockeado),
calcula el resumen, lo sube, y que -si el dia es domingo- ADEMAS guarda el
snapshot semanal con el nombre correcto."""
from datetime import datetime, timedelta, timezone
from io import BytesIO
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

import reclamos.bp_reclamos_gold as gold_mod
from tests._fakes_azure import ContenedorGoldFalso, ContenedorSilverFalso, blob_service_client_falso


@pytest.fixture(autouse=True)
def _env_falso(monkeypatch):
    monkeypatch.setenv("STORAGE_CONNECTION_STRING", "conexion-de-mentira")


def _tabla_silver_de_prueba() -> bytes:
    creado = datetime(2026, 9, 1, tzinfo=timezone.utc)
    filas = [
        {
            "reclamoId": "R-001", "barrio": "Flores", "categoria": "BACHES",
            "prioridad": "MEDIA", "origenClasificacion": "CIUDADANO", "estado_actual": "CERRADO",
            "fecha_creado": creado, "fecha_cerrado": creado + timedelta(hours=10),
        },
        {
            "reclamoId": "R-002", "barrio": "Flores", "categoria": "BACHES",
            "prioridad": "MEDIA", "origenClasificacion": "CIUDADANO", "estado_actual": "CERRADO",
            "fecha_creado": creado, "fecha_cerrado": creado + timedelta(hours=30),
        },
    ]
    df = pd.DataFrame(filas)
    for col in ("fecha_creado", "fecha_cerrado"):
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
        gold_mod.reclamos_silver_a_gold(MagicMock())

    # 1. Se escribio el acumulado, con los 2 reclamos agrupados (promedio (10+30)/2 = 20hs).
    acumulado = gold._blobs["Reclamos/reclamos_resumen.parquet"]
    assert len(acumulado.subidas) == 1
    df_resumen = pd.read_parquet(BytesIO(acumulado.subidas[0]))
    assert df_resumen.iloc[0]["row_count"] == 2
    assert df_resumen.iloc[0]["tiempo_prom_hasta_estado_actual"] == 20.0

    # 2. Como fecha_domingo es domingo, TAMBIEN se guardo el snapshot semanal.
    anio, semana, _ = fecha_domingo.isocalendar()
    nombre_snapshot = f"Reclamos/reclamos_resumen_{semana:02d}_{anio}.parquet"
    assert nombre_snapshot in gold._blobs
    assert len(gold._blobs[nombre_snapshot].subidas) == 1
