"""Silver de Espacios: la maquina de estados es simple, de un solo salto
(PENDIENTE -> CONFIRMADA|CANCELADA), y el modelo solo admite UNA transicion
por reserva -- una segunda transicion se ignora."""
from datetime import datetime, timezone

from espacios.bp_espacios_silver import aplicar_evento, dataframe_vacio


def _evento_reserva_creada(reserva_id: str, creado_en: datetime) -> dict:
    return {
        "metadata": {"eventType": "com.citypass.espacios.ReservaCreada"},
        "data": {
            "reservaId": reserva_id,
            "tipoReserva": "EVENTO",
            "recursoId": "evento-cultural-01",
            "categoria": "CULTURAL",
            "zona": "Palermo",
            "fechaHora": creado_en.isoformat(),
            "cantidadPersonas": 1,
            "cupoMaximo": 10,
            "ciudadanoId": "user-1",
            "organizadorId": None,
            "createdAt": creado_en.isoformat(),
        },
    }


def _evento_reserva_actualizada(reserva_id: str, estado_nuevo: str, fecha: datetime) -> dict:
    return {
        "metadata": {"eventType": "com.citypass.espacios.ReservaActualizada"},
        "data": {
            "reservaId": reserva_id,
            "estadoAnterior": "PENDIENTE",
            "estadoNuevo": estado_nuevo,
            "updatedAt": fecha.isoformat(),
        },
    }


def test_reserva_creada_nueva_queda_pendiente():
    df = dataframe_vacio()
    df = aplicar_evento(df, _evento_reserva_creada("RES-001", datetime(2026, 9, 1, tzinfo=timezone.utc)))

    fila = df.iloc[0]
    assert fila["estado_actual"] == "PENDIENTE"
    assert fila["confirmada"] == False  # noqa: E712
    assert fila["cancelada"] == False  # noqa: E712


def test_actualizada_duplicada_no_permite_una_segunda_transicion():
    df = dataframe_vacio()
    df = aplicar_evento(df, _evento_reserva_creada("RES-002", datetime(2026, 9, 1, tzinfo=timezone.utc)))
    df = aplicar_evento(
        df, _evento_reserva_actualizada("RES-002", "CONFIRMADA", datetime(2026, 9, 2, tzinfo=timezone.utc))
    )

    # El modelo solo admite una transicion -- si llega otra (ej. alguien
    # intenta cancelarla despues de confirmada), se ignora.
    df = aplicar_evento(
        df, _evento_reserva_actualizada("RES-002", "CANCELADA", datetime(2026, 9, 3, tzinfo=timezone.utc))
    )

    fila = df[df["reservaId"] == "RES-002"].iloc[0]
    assert fila["estado_actual"] == "CONFIRMADA"
    assert fila["confirmada"] == True  # noqa: E712
    assert fila["cancelada"] == False  # noqa: E712


def test_reserva_actualizada_antes_que_creada_no_pisa_el_estado_con_pendiente():
    df = dataframe_vacio()
    df = aplicar_evento(
        df, _evento_reserva_actualizada("RES-003", "CONFIRMADA", datetime(2026, 9, 2, tzinfo=timezone.utc))
    )
    assert df.iloc[0]["estado_actual"] == "CONFIRMADA"

    # Llega despues (fuera de orden) la ReservaCreada.
    df = aplicar_evento(df, _evento_reserva_creada("RES-003", datetime(2026, 9, 1, tzinfo=timezone.utc)))

    fila = df[df["reservaId"] == "RES-003"].iloc[0]
    # ReservaCreada "normalmente" pondria PENDIENTE, pero como ya habia un
    # estado_actual cargado, no lo pisa.
    assert fila["estado_actual"] == "CONFIRMADA"
