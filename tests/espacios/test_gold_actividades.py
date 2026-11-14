"""Gold de Espacios: Actividades. El resumen cuenta actividades y suma los
contadores de causa por estado_actual/categoria/causaTipo/organizadorTipo,
a partir de una tabla de Silver armada a mano. No lee ni escribe ningun
archivo, no se conecta a Azure.
"""
from datetime import datetime, timezone

import pandas as pd

from espacios.bp_espacios_gold import calcular_resumen_actividades


def _fila(
    actividad_id, estado_actual="PROGRAMADA", categoria="CULTURAL", causa_tipo=None,
    organizador_tipo=None, capacidad=20, barrio=None,
    veces_evento_climatico=0, veces_reclamo=0, veces_incidente_vial=0, veces_accidente_vial=0,
):
    return {
        "actividadId": actividad_id,
        "estado_actual": estado_actual,
        "categoria": categoria,
        "causaTipo": causa_tipo,
        "organizadorTipo": organizador_tipo,
        "capacidad": capacidad,
        "barrio": barrio,
        "masivo": capacidad >= 500,
        "veces_evento_climatico": veces_evento_climatico,
        "veces_reclamo": veces_reclamo,
        "veces_incidente_vial": veces_incidente_vial,
        "veces_accidente_vial": veces_accidente_vial,
    }


def test_caso_1_culturales_canceladas_por_clima():
    filas = [
        _fila("A-001", estado_actual="CANCELADA", categoria="CULTURAL", causa_tipo="EVENTO_CLIMATICO"),
        _fila("A-002", estado_actual="CANCELADA", categoria="CULTURAL", causa_tipo="EVENTO_CLIMATICO"),
        _fila("A-003", estado_actual="CANCELADA", categoria="DEPORTIVO", causa_tipo="EVENTO_CLIMATICO"),
    ]
    df_silver = pd.DataFrame(filas)

    resumen = calcular_resumen_actividades(df_silver, fecha_snapshot=datetime(2026, 9, 5, tzinfo=timezone.utc))

    caso_1 = resumen[
        (resumen["estado_actual"] == "CANCELADA")
        & (resumen["categoria"] == "CULTURAL")
        & (resumen["causaTipo"] == "EVENTO_CLIMATICO")
    ]
    assert caso_1["cantidadActividades"].sum() == 2


def test_caso_3_masivas_cuenta_capacidad_mayor_o_igual_500():
    filas = [
        _fila("A-004", capacidad=500),
        _fila("A-005", capacidad=499),
        _fila("A-006", capacidad=5000),
    ]
    df_silver = pd.DataFrame(filas)

    resumen = calcular_resumen_actividades(df_silver, fecha_snapshot=datetime(2026, 9, 5, tzinfo=timezone.utc))

    assert resumen["cantidadMasivas"].sum() == 2


def test_caso_4_y_5_distinguen_actividades_distintas_de_eventos_totales():
    # A-007 fue afectada 1 sola vez por incidente vial; A-008, 3 veces
    # (pero solo la primera determina su causaTipo "ultimo conocido" en
    # silver si las demas no revierten el estado -- lo que importa para
    # caso 5 es el contador, no cuantas filas distintas hay).
    filas = [
        _fila("A-007", causa_tipo="INCIDENTE_VIAL", veces_incidente_vial=1),
        _fila("A-008", causa_tipo="INCIDENTE_VIAL", veces_incidente_vial=3),
    ]
    df_silver = pd.DataFrame(filas)

    resumen = calcular_resumen_actividades(df_silver, fecha_snapshot=datetime(2026, 9, 5, tzinfo=timezone.utc))

    caso_4 = resumen[resumen["causaTipo"] == "INCIDENTE_VIAL"]
    assert caso_4["cantidadActividades"].sum() == 2  # actividades distintas
    assert resumen["totalVecesIncidenteVial"].sum() == 4  # eventos totales


def test_caso_5_accidente_vial_es_distinto_de_incidente_vial_del_caso_4():
    # Confirmado con datos reales: ACCIDENTE_VIAL no es un typo de
    # INCIDENTE_VIAL, son dos causaTipo distintos con sus propias metricas.
    filas = [
        _fila("A-012", causa_tipo="INCIDENTE_VIAL", veces_incidente_vial=2, veces_accidente_vial=0),
        _fila("A-013", causa_tipo="ACCIDENTE_VIAL", veces_incidente_vial=0, veces_accidente_vial=3),
    ]
    df_silver = pd.DataFrame(filas)

    resumen = calcular_resumen_actividades(df_silver, fecha_snapshot=datetime(2026, 9, 5, tzinfo=timezone.utc))

    assert resumen["totalVecesIncidenteVial"].sum() == 2
    assert resumen["totalVecesAccidenteVial"].sum() == 3


def test_agrupa_por_barrio_y_categoria():
    filas = [
        _fila("A-016", barrio="Caballito", categoria="CULTURAL"),
        _fila("A-017", barrio="Caballito", categoria="CULTURAL"),
        _fila("A-018", barrio="Caballito", categoria="DEPORTIVO"),
        _fila("A-019", barrio="Recoleta", categoria="CULTURAL"),
    ]
    df_silver = pd.DataFrame(filas)

    resumen = calcular_resumen_actividades(df_silver, fecha_snapshot=datetime(2026, 9, 5, tzinfo=timezone.utc))

    caballito_cultural = resumen[
        (resumen["barrio"] == "Caballito") & (resumen["categoria"] == "CULTURAL")
    ]["cantidadActividades"].sum()
    caballito_deportivo = resumen[
        (resumen["barrio"] == "Caballito") & (resumen["categoria"] == "DEPORTIVO")
    ]["cantidadActividades"].sum()
    recoleta_cultural = resumen[
        (resumen["barrio"] == "Recoleta") & (resumen["categoria"] == "CULTURAL")
    ]["cantidadActividades"].sum()

    assert caballito_cultural == 2
    assert caballito_deportivo == 1
    assert recoleta_cultural == 1


def test_caso_6_agrupa_por_organizadortipo():
    filas = [
        _fila("A-009", organizador_tipo="CIUDADANO"),
        _fila("A-010", organizador_tipo="CIUDADANO"),
        _fila("A-011", organizador_tipo="CIUDAD"),
    ]
    df_silver = pd.DataFrame(filas)

    resumen = calcular_resumen_actividades(df_silver, fecha_snapshot=datetime(2026, 9, 5, tzinfo=timezone.utc))

    ciudadano = resumen[resumen["organizadorTipo"] == "CIUDADANO"]["cantidadActividades"].sum()
    ciudad = resumen[resumen["organizadorTipo"] == "CIUDAD"]["cantidadActividades"].sum()
    assert ciudadano == 2
    assert ciudad == 1
