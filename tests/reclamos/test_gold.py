"""Test 3: el resumen de Gold cuenta y promedia bien, a partir de una tabla
de Silver armada a mano (3 reclamos inventados, con fechas conocidas).

No lee ni escribe ningun archivo, no se conecta a Azure.
"""
from datetime import datetime, timedelta, timezone

import pandas as pd

from reclamos.bp_reclamos_gold import calcular_resumen


def _fila(reclamo_id: str, creado: datetime, cerrado: datetime) -> dict:
    return {
        "reclamoId": reclamo_id,
        "barrio": "Flores",
        "categoria": "BACHES",
        "prioridad": "MEDIA",
        "origenClasificacion": "CIUDADANO",
        "estado_actual": "CERRADO",
        "fecha_creado": creado,
        "fecha_cerrado": cerrado,
    }


def test_calcular_resumen_cuenta_y_promedia_correctamente():
    creado = datetime(2026, 9, 1, 0, 0, tzinfo=timezone.utc)

    # 3 reclamos de la misma categoria/barrio/estado, que tardaron
    # 10, 20 y 30 horas en cerrarse -> promedio esperado a mano: 20 horas.
    filas = [
        _fila("R-001", creado, creado + timedelta(hours=10)),
        _fila("R-002", creado, creado + timedelta(hours=20)),
        _fila("R-003", creado, creado + timedelta(hours=30)),
    ]
    df_silver = pd.DataFrame(filas)
    df_silver["fecha_creado"] = pd.to_datetime(df_silver["fecha_creado"], utc=True)
    df_silver["fecha_cerrado"] = pd.to_datetime(df_silver["fecha_cerrado"], utc=True)

    resumen = calcular_resumen(df_silver, fecha_snapshot=datetime(2026, 9, 5, tzinfo=timezone.utc))

    assert len(resumen) == 1
    fila = resumen.iloc[0]
    assert fila["row_count"] == 3
    assert fila["tiempo_prom_hasta_estado_actual"] == 20.0
