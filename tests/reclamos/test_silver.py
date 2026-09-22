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
