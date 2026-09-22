"""Gold de Emergencias: el tiempo promedio de respuesta se calcula UNA VEZ
por prioridad (no por combinacion estado+prioridad) para no diluir el
promedio -- este test comprueba justamente esa regla, que es la parte no
obvia del calculo (esta explicada en el docstring del propio archivo real).
"""
from datetime import datetime, timedelta, timezone

import pandas as pd

from emergencias.bp_emergencias_gold import calcular_resumen


def test_tiempo_prom_respuesta_se_calcula_por_prioridad_no_por_combinacion_de_estado():
    creado = datetime(2026, 9, 1, tzinfo=timezone.utc)
    filas = [
        {
            "emergenciaId": "E-001", "estado_actual": "DESPACHADA", "prioridad": "ALTA",
            "fecha_creada": creado, "fecha_despachada": creado + timedelta(minutes=10),
            "fecha_en_lugar": pd.NaT,
        },
        {
            "emergenciaId": "E-002", "estado_actual": "CERRADA", "prioridad": "ALTA",
            "fecha_creada": creado, "fecha_despachada": creado + timedelta(minutes=20),
            "fecha_en_lugar": creado + timedelta(minutes=30),
        },
    ]
    df_silver = pd.DataFrame(filas)
    for col in ("fecha_creada", "fecha_despachada", "fecha_en_lugar"):
        df_silver[col] = pd.to_datetime(df_silver[col], utc=True)

    resumen = calcular_resumen(df_silver, fecha_snapshot=datetime(2026, 9, 5, tzinfo=timezone.utc))

    # Una fila por estado_actual (2), pero el promedio de despacho es el
    # MISMO en las dos: se calcula sobre las 2 emergencias ALTA juntas
    # (10+20)/2=15, no por separado dentro de cada estado.
    assert len(resumen) == 2
    assert (resumen["tiempoPromRespuestaDespacho"] == 15.0).all()
