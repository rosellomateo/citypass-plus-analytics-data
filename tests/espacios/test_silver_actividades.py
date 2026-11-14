"""Silver de Espacios: Actividades (ActividadCreada/ActividadModificada).
A diferencia de Reservas (en test_silver.py), una actividad SI puede
revisitar un estado -- esto comprueba que estado_actual sale de la
transicion con fecha mas reciente, no de "la primera" ni de un enum fijo.
"""
from datetime import datetime, timezone

import pandas as pd

from espacios.bp_espacios_silver import (
    aplicar_evento_actividad,
    dataframe_vacio_actividades,
)


def _epoch_ms(momento: datetime) -> int:
    return int(momento.timestamp() * 1000)


def _evento_actividad_creada(
    actividad_id: str, estado: str = "PROGRAMADA", categoria: str | None = "CULTURAL",
    lat: float = -34.6, lng: float = -58.4, capacidad: int = 20,
    organizador_tipo: str | None = "CIUDADANO",
) -> dict:
    return {
        "metadata": {"eventType": "com.citypass.espacios.ActividadCreada"},
        "data": {
            "actividadId": actividad_id,
            "correlationId": f"act-{actividad_id}",
            "titulo": "Taller de prueba",
            "estado": estado,
            "capacidad": capacidad,
            "fechaHoraInicio": _epoch_ms(datetime(2026, 11, 20, tzinfo=timezone.utc)),
            "fechaHoraFinalizacion": _epoch_ms(datetime(2026, 11, 20, 2, 0, tzinfo=timezone.utc)),
            "ubicacion": {"lat": lat, "lng": lng},
            "direccion": "Av. Siempre Viva 123",
            "espacioNombre": "Centro Cultural",
            "actorSub": "U-001",
            "categoria": categoria,
            "organizadorTipo": organizador_tipo,
        },
    }


def _evento_actividad_modificada(
    actividad_id: str, estado_anterior: str, estado_nuevo: str, modificada_en: datetime,
    motivo: str | None = None, causa_id: str | None = None, causa_tipo: str | None = None,
    event_id: str | None = None,
) -> dict:
    return {
        "metadata": {"eventType": "com.citypass.espacios.ActividadModificada", "eventId": event_id},
        "data": {
            "actividadId": actividad_id,
            "correlationId": f"act-{actividad_id}",
            "estadoAnterior": estado_anterior,
            "estadoNuevo": estado_nuevo,
            "motivo": motivo,
            "causaId": causa_id,
            "causaTipo": causa_tipo,
            "modificadaEn": _epoch_ms(modificada_en),
            "actorSub": "U-001",
        },
    }


def test_actividad_creada_nueva_toma_el_estado_del_evento():
    df = dataframe_vacio_actividades()
    df = aplicar_evento_actividad(df, _evento_actividad_creada("A-001"))

    assert len(df) == 1
    fila = df.iloc[0]
    assert fila["actividadId"] == "A-001"
    assert fila["estado_actual"] == "PROGRAMADA"
    assert fila["latitud"] == -34.6
    assert fila["longitud"] == -58.4
    assert fila["organizadorTipo"] == "CIUDADANO"
    assert fila["barrio"] == "Balvanera"
    assert fila["masivo"] == False  # noqa: E712 -- capacidad=20 por default


def test_barrio_se_deriva_de_lat_lng_por_point_in_polygon():
    df = dataframe_vacio_actividades()
    # Centro Cultural Recoleta, coordenada real vista en produccion.
    df = aplicar_evento_actividad(
        df, _evento_actividad_creada("A-014", lat=-34.5862976, lng=-58.3925127),
    )
    df = aplicar_evento_actividad(
        df, _evento_actividad_creada("A-015", lat=0, lng=0),
    )

    assert df[df["actividadId"] == "A-014"].iloc[0]["barrio"] == "Recoleta"
    assert df[df["actividadId"] == "A-015"].iloc[0]["barrio"] is None


def test_capacidad_mayor_o_igual_500_marca_masivo_true():
    df = dataframe_vacio_actividades()
    df = aplicar_evento_actividad(df, _evento_actividad_creada("A-012", capacidad=500))
    df = aplicar_evento_actividad(df, _evento_actividad_creada("A-013", capacidad=499))

    assert df[df["actividadId"] == "A-012"].iloc[0]["masivo"] == True  # noqa: E712
    assert df[df["actividadId"] == "A-013"].iloc[0]["masivo"] == False  # noqa: E712


def test_modificada_actualiza_estado_y_queda_con_la_transicion_mas_reciente():
    df = dataframe_vacio_actividades()
    df = aplicar_evento_actividad(df, _evento_actividad_creada("A-002"))

    df = aplicar_evento_actividad(
        df,
        _evento_actividad_modificada(
            "A-002", "PROGRAMADA", "SUSPENDIDA", datetime(2026, 11, 15, tzinfo=timezone.utc),
            causa_tipo="EVENTO_CLIMATICO",
        ),
    )
    # Se revisita PROGRAMADA -- tiene que ganar, es la transicion mas nueva.
    df = aplicar_evento_actividad(
        df,
        _evento_actividad_modificada(
            "A-002", "SUSPENDIDA", "PROGRAMADA", datetime(2026, 11, 16, tzinfo=timezone.utc),
        ),
    )

    fila = df[df["actividadId"] == "A-002"].iloc[0]
    assert fila["estado_actual"] == "PROGRAMADA"
    assert fila["fecha_actualizacion"] == pd.Timestamp(datetime(2026, 11, 16, tzinfo=timezone.utc))
    # causaTipo de la transicion vieja no se borra con la nueva (que no trae causa).
    assert fila["causaTipo"] == "EVENTO_CLIMATICO"


def test_modificada_fuera_de_orden_no_pisa_el_estado_mas_nuevo():
    df = dataframe_vacio_actividades()
    df = aplicar_evento_actividad(df, _evento_actividad_creada("A-003"))

    df = aplicar_evento_actividad(
        df,
        _evento_actividad_modificada(
            "A-003", "PROGRAMADA", "CANCELADA", datetime(2026, 11, 16, tzinfo=timezone.utc),
        ),
    )
    # Llega tarde (fuera de orden) una transicion mas vieja -- no tiene que pisar CANCELADA.
    df = aplicar_evento_actividad(
        df,
        _evento_actividad_modificada(
            "A-003", "PROGRAMADA", "SUSPENDIDA", datetime(2026, 11, 15, tzinfo=timezone.utc),
        ),
    )

    fila = df[df["actividadId"] == "A-003"].iloc[0]
    assert fila["estado_actual"] == "CANCELADA"


def test_modificada_antes_que_creada_crea_placeholder():
    df = dataframe_vacio_actividades()
    df = aplicar_evento_actividad(
        df,
        _evento_actividad_modificada(
            "A-004", "PROGRAMADA", "CANCELADA", datetime(2026, 11, 16, tzinfo=timezone.utc),
        ),
    )

    assert len(df) == 1
    fila = df.iloc[0]
    assert fila["actividadId"] == "A-004"
    assert fila["estado_actual"] == "CANCELADA"
    assert pd.isna(fila["titulo"])


def test_contador_de_causa_suma_una_vez_por_evento_aunque_no_sea_la_transicion_mas_reciente():
    df = dataframe_vacio_actividades()
    df = aplicar_evento_actividad(df, _evento_actividad_creada("A-010"))

    df = aplicar_evento_actividad(
        df,
        _evento_actividad_modificada(
            "A-010", "PROGRAMADA", "SUSPENDIDA", datetime(2026, 11, 16, tzinfo=timezone.utc),
            causa_tipo="INCIDENTE_VIAL", event_id="evt-1",
        ),
    )
    # Llega despues una transicion mas vieja (fuera de orden) con otra
    # causa -- no determina estado_actual, pero el contador de su causa
    # tiene que sumar igual.
    df = aplicar_evento_actividad(
        df,
        _evento_actividad_modificada(
            "A-010", "PROGRAMADA", "SUSPENDIDA", datetime(2026, 11, 15, tzinfo=timezone.utc),
            causa_tipo="RECLAMO", event_id="evt-2",
        ),
    )

    fila = df[df["actividadId"] == "A-010"].iloc[0]
    assert fila["veces_incidente_vial"] == 1
    assert fila["veces_reclamo"] == 1
    assert fila["veces_evento_climatico"] == 0


def test_contador_no_duplica_con_reentrega_del_mismo_eventid():
    df = dataframe_vacio_actividades()
    df = aplicar_evento_actividad(df, _evento_actividad_creada("A-011"))

    evento = _evento_actividad_modificada(
        "A-011", "PROGRAMADA", "SUSPENDIDA", datetime(2026, 11, 16, tzinfo=timezone.utc),
        causa_tipo="INCIDENTE_VIAL", event_id="evt-reentregado",
    )
    df = aplicar_evento_actividad(df, evento)
    df = aplicar_evento_actividad(df, evento)  # Kafka reentrega el mismo evento.

    fila = df[df["actividadId"] == "A-011"].iloc[0]
    assert fila["veces_incidente_vial"] == 1


def test_evento_de_reserva_no_lo_procesa_este_dispatcher():
    df = dataframe_vacio_actividades()
    evento_reserva = {
        "metadata": {"eventType": "com.citypass.espacios.ReservaCreada"},
        "data": {"reservaId": "R-001"},
    }

    resultado = aplicar_evento_actividad(df, evento_reserva)

    assert resultado is None
