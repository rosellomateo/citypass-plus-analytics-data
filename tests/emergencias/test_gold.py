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
            "categoria": "INCENDIO", "causaTipo": None, "afectaCirculacion": False,
            "prioridadOriginal": None,
            "fecha_creada": creado, "fecha_despachada": creado + timedelta(minutes=10),
            "fecha_en_lugar": pd.NaT,
        },
        {
            "emergenciaId": "E-002", "estado_actual": "CERRADA", "prioridad": "ALTA",
            "categoria": "INCENDIO", "causaTipo": None, "afectaCirculacion": False,
            "prioridadOriginal": None,
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


def _fila(emergencia_id, causa_tipo=None, categoria="INCENDIO", afecta_circulacion=False,
          prioridad="ALTA", prioridad_original=None, estado_actual="CERRADA"):
    creado = datetime(2026, 9, 1, tzinfo=timezone.utc)
    return {
        "emergenciaId": emergencia_id, "estado_actual": estado_actual, "prioridad": prioridad,
        "categoria": categoria, "causaTipo": causa_tipo, "afectaCirculacion": afecta_circulacion,
        "prioridadOriginal": prioridad_original,
        "fecha_creada": creado, "fecha_despachada": creado + timedelta(minutes=10),
        "fecha_en_lugar": creado + timedelta(minutes=15),
    }


def _df_silver(filas):
    df = pd.DataFrame(filas)
    for col in ("fecha_creada", "fecha_despachada", "fecha_en_lugar"):
        df[col] = pd.to_datetime(df[col], utc=True)
    return df


def test_resumen_separa_por_causatipo_categoria_y_afectacirculacion():
    df_silver = _df_silver([
        _fila("E-001", causa_tipo="EVENTO_CLIMATICO"),
        _fila("E-002", causa_tipo="RECLAMO"),
        _fila("E-003", categoria="PREVENTIVA", causa_tipo="EVENTO_PUBLICO"),
        _fila("E-004", afecta_circulacion=True, causa_tipo="INCIDENTE_VIAL"),
        _fila("E-005", causa_tipo="RESERVA"),
        _fila("E-006"),  # organica, sin causa
    ])

    resumen = calcular_resumen(df_silver, fecha_snapshot=datetime(2026, 9, 5, tzinfo=timezone.utc))

    assert len(resumen) == 6
    assert set(resumen["causaTipo"].dropna()) == {"EVENTO_CLIMATICO", "RECLAMO", "EVENTO_PUBLICO", "INCIDENTE_VIAL", "RESERVA"}
    assert resumen[resumen["categoria"] == "PREVENTIVA"]["cantidadEmergencias"].iloc[0] == 1
    assert resumen[resumen["afectaCirculacion"] == True]["cantidadEmergencias"].iloc[0] == 1


def test_criticidad_afectada_cuenta_solo_las_que_cambiaron_de_prioridad():
    df_silver = _df_silver([
        _fila("E-001", causa_tipo="EVENTO_CLIMATICO", prioridad="ALTA", prioridad_original="MEDIA"),
        _fila("E-002", causa_tipo="EVENTO_CLIMATICO", prioridad="MEDIA", prioridad_original="MEDIA"),
        _fila("E-003", causa_tipo="EVENTO_CLIMATICO", prioridad="ALTA", prioridad_original="BAJA"),
    ])
    # Las 3 van al mismo grupo (mismo estado/prioridad/categoria/causaTipo/afectaCirculacion)
    # salvo que prioridad difiere -> separa E-002 de las otras dos.

    resumen = calcular_resumen(df_silver, fecha_snapshot=datetime(2026, 9, 5, tzinfo=timezone.utc))

    fila_alta = resumen[resumen["prioridad"] == "ALTA"].iloc[0]
    assert fila_alta["cantidadEmergencias"] == 2
    assert fila_alta["criticidadAfectada"] == 2

    fila_media = resumen[resumen["prioridad"] == "MEDIA"].iloc[0]
    assert fila_media["cantidadEmergencias"] == 1
    assert fila_media["criticidadAfectada"] == 0


def test_criticidad_afectada_ignora_emergencias_sin_prioridadoriginal():
    df_silver = _df_silver([
        _fila("E-001", prioridad="ALTA", prioridad_original=None),
    ])

    resumen = calcular_resumen(df_silver, fecha_snapshot=datetime(2026, 9, 5, tzinfo=timezone.utc))

    assert resumen.iloc[0]["criticidadAfectada"] == 0


def test_tiempo_prom_resolucion_se_calcula_por_causa_tipo():
    """Caso 5: el tiempo de resolucion de las emergencias en bici sale solo de
    las MOVILIDAD_BICICLETA resueltas y se repite en todas sus filas (tambien
    en la de las pendientes); las de otra causa no lo afectan."""
    df_silver = _df_silver([
        _fila("E-001", causa_tipo="MOVILIDAD_BICICLETA", estado_actual="RESUELTA"),
        _fila("E-002", causa_tipo="MOVILIDAD_BICICLETA", estado_actual="RESUELTA"),
        _fila("E-003", causa_tipo="MOVILIDAD_BICICLETA", estado_actual="PENDIENTE"),
        _fila("E-004", causa_tipo=None, estado_actual="RESUELTA"),
    ])
    creado = datetime(2026, 9, 1, tzinfo=timezone.utc)
    df_silver["fecha_resuelta"] = pd.to_datetime([
        creado + timedelta(minutes=10),
        creado + timedelta(minutes=20),
        pd.NaT,
        creado + timedelta(minutes=100),
    ], utc=True)

    resumen = calcular_resumen(df_silver, fecha_snapshot=datetime(2026, 9, 5, tzinfo=timezone.utc))

    bici = resumen[resumen["causaTipo"] == "MOVILIDAD_BICICLETA"]
    assert len(bici) == 2  # una fila RESUELTA y otra PENDIENTE
    assert (bici["tiempoPromResolucion"] == 15.0).all()
    sin_causa = resumen[resumen["causaTipo"].isna()]
    assert (sin_causa["tiempoPromResolucion"] == 100.0).all()
