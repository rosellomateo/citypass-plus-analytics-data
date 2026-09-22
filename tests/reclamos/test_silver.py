"""Test 2: como se arma el historial de un reclamo en Silver, a partir de
eventos ReclamoCreado / ReclamoActualizado.

Todo en memoria, con eventos inventados. No lee ni escribe ningun archivo,
no se conecta a Azure.
"""
from datetime import datetime, timedelta, timezone

import pandas as pd

from reclamos.bp_reclamos_silver import aplicar_evento, dataframe_vacio


def _epoch_ms(momento: datetime) -> int:
    return int(momento.timestamp() * 1000)


def _evento_creado(reclamo_id: str, creado_en: datetime, categoria: str = "BACHES") -> dict:
    return {
        "metadata": {
            "eventType": "com.citypass.reclamos.ReclamoCreado",
            "eventId": f"evt-creado-{reclamo_id}",
        },
        "data": {
            "reclamoId": reclamo_id,
            "categoria": categoria,
            "createdAt": _epoch_ms(creado_en),
        },
    }


def _evento_actualizado(
    reclamo_id: str, estado_anterior: str, estado_nuevo: str, fecha: datetime, event_id: str
) -> dict:
    return {
        "metadata": {
            "eventType": "com.citypass.reclamos.ReclamoActualizado",
            "eventId": event_id,
        },
        "data": {
            "reclamoId": reclamo_id,
            "estadoAnterior": estado_anterior,
            "estadoNuevo": estado_nuevo,
            "updatedAt": _epoch_ms(fecha),
        },
    }


def test_reclamo_creado_nuevo_crea_fila_en_recibido():
    df = dataframe_vacio()
    creado_en = datetime(2026, 9, 1, 8, 0, tzinfo=timezone.utc)

    df = aplicar_evento(df, _evento_creado("R-001", creado_en))

    assert len(df) == 1
    fila = df.iloc[0]
    assert fila["reclamoId"] == "R-001"
    assert fila["estado_actual"] == "RECIBIDO"
    assert fila["fecha_creado"] == pd.Timestamp(creado_en)


def test_reclamo_creado_duplicado_no_crea_segunda_fila_ni_pisa_datos():
    df = dataframe_vacio()
    creado_en = datetime(2026, 9, 1, 8, 0, tzinfo=timezone.utc)
    evento = _evento_creado("R-001", creado_en, categoria="BACHES")

    df = aplicar_evento(df, evento)
    # Se reenvia el mismo evento (ej: la red fallo y el emisor reintento).
    df = aplicar_evento(df, evento)

    assert len(df) == 1
    assert df.iloc[0]["categoria"] == "BACHES"


def test_estado_final_depende_de_la_fecha_del_evento_no_del_orden_de_llegada():
    df = dataframe_vacio()
    df = aplicar_evento(df, _evento_creado("R-002", datetime(2026, 9, 1, tzinfo=timezone.utc)))

    # Llega PRIMERO (en el tiempo real) el evento de CERRADO, con fecha 12/sep.
    df = aplicar_evento(
        df,
        _evento_actualizado(
            "R-002", "RESUELTO", "CERRADO",
            datetime(2026, 9, 12, tzinfo=timezone.utc),
            event_id="evt-cerrado",
        ),
    )
    # Llega DESPUES (en el tiempo real) el evento de RESUELTO, con fecha 10/sep
    # (mas vieja que la que ya estaba cargada).
    df = aplicar_evento(
        df,
        _evento_actualizado(
            "R-002", "EN_PROCESO", "RESUELTO",
            datetime(2026, 9, 10, tzinfo=timezone.utc),
            event_id="evt-resuelto",
        ),
    )

    fila = df[df["reclamoId"] == "R-002"].iloc[0]
    # Si esto diera "RESUELTO", significaria que el codigo mira que evento
    # llego ultimo en el tiempo real, en vez de mirar la fecha que trae cada
    # evento -- que es la regla de negocio correcta.
    assert fila["estado_actual"] == "CERRADO"


def test_creado_duplicado_con_createdat_mas_nuevo_no_reemplaza_al_original():
    # Bug real que encontramos: dos ReclamoCreado subidos casi al mismo
    # tiempo pueden desempatar por orden de blob (practicamente al azar) en
    # vez de por cual paso primero de verdad. La regla correcta usa el
    # createdAt de cada evento, no el orden de procesamiento.
    df = dataframe_vacio()
    original = _evento_creado("R-003", datetime(2026, 9, 1, 8, 0, tzinfo=timezone.utc), categoria="BACHES")
    reenvio_con_correccion_tardia = _evento_creado(
        "R-003", datetime(2026, 9, 1, 8, 5, tzinfo=timezone.utc), categoria="OTROS"
    )

    df = aplicar_evento(df, original)
    df = aplicar_evento(df, reenvio_con_correccion_tardia)

    fila = df[df["reclamoId"] == "R-003"].iloc[0]
    assert fila["categoria"] == "BACHES"
    assert fila["fecha_creado"] == pd.Timestamp(datetime(2026, 9, 1, 8, 0, tzinfo=timezone.utc))


def test_creado_duplicado_con_createdat_mas_antiguo_reemplaza_al_ya_cargado():
    # Caso inverso: si el "duplicado" tiene un createdAt MAS VIEJO que el ya
    # aplicado, es el que realmente deberia haber ganado -- se reemplazan
    # los datos base por los del evento mas antiguo.
    df = dataframe_vacio()
    procesado_primero = _evento_creado("R-004", datetime(2026, 9, 1, 8, 5, tzinfo=timezone.utc), categoria="OTROS")
    llega_despues_pero_es_mas_viejo = _evento_creado(
        "R-004", datetime(2026, 9, 1, 8, 0, tzinfo=timezone.utc), categoria="BACHES"
    )

    df = aplicar_evento(df, procesado_primero)
    df = aplicar_evento(df, llega_despues_pero_es_mas_viejo)

    fila = df[df["reclamoId"] == "R-004"].iloc[0]
    assert fila["categoria"] == "BACHES"
    assert fila["fecha_creado"] == pd.Timestamp(datetime(2026, 9, 1, 8, 0, tzinfo=timezone.utc))


def test_actualizado_con_mismo_eventid_reenviado_se_ignora():
    df = dataframe_vacio()
    df = aplicar_evento(df, _evento_creado("R-005", datetime(2026, 9, 1, tzinfo=timezone.utc)))
    asignado = _evento_actualizado(
        "R-005", "RECIBIDO", "ASIGNADO", datetime(2026, 9, 2, tzinfo=timezone.utc), event_id="evt-asignado"
    )
    df = aplicar_evento(df, asignado)

    # Reentrega real del MISMO evento (mismo eventId) -- debe ignorarse,
    # aunque el modelo permite revisitar estados (no alcanza con mirar si
    # ASIGNADO ya tiene fecha).
    reentrega = _evento_actualizado(
        "R-005", "RECIBIDO", "ASIGNADO", datetime(2026, 9, 2, 10, 0, tzinfo=timezone.utc), event_id="evt-asignado"
    )
    df = aplicar_evento(df, reentrega)

    fila = df[df["reclamoId"] == "R-005"].iloc[0]
    assert fila["fecha_asignado"] == pd.Timestamp(datetime(2026, 9, 2, tzinfo=timezone.utc))


def test_revisita_legitima_al_mismo_estado_con_eventid_distinto_se_aplica():
    # A diferencia de movilidad/residuos, reclamos SI permite volver a un
    # estado ya visitado (ej. EN_REVISION -> RECIBIDO esta documentado como
    # valido). Un evento nuevo (eventId distinto) que revisita RECIBIDO no
    # tiene que bloquearse como si fuera un duplicado.
    df = dataframe_vacio()
    df = aplicar_evento(df, _evento_creado("R-006", datetime(2026, 9, 1, tzinfo=timezone.utc)))
    df = aplicar_evento(
        df,
        _evento_actualizado(
            "R-006", "RECIBIDO", "EN_REVISION", datetime(2026, 9, 2, tzinfo=timezone.utc), event_id="evt-en-revision"
        ),
    )
    # Vuelve a RECIBIDO, con un eventId nuevo -- transicion legitima.
    df = aplicar_evento(
        df,
        _evento_actualizado(
            "R-006", "EN_REVISION", "RECIBIDO", datetime(2026, 9, 3, tzinfo=timezone.utc), event_id="evt-vuelve-recibido"
        ),
    )

    fila = df[df["reclamoId"] == "R-006"].iloc[0]
    assert fila["estado_actual"] == "RECIBIDO"
    assert fila["fecha_creado"] == pd.Timestamp(datetime(2026, 9, 3, tzinfo=timezone.utc))
