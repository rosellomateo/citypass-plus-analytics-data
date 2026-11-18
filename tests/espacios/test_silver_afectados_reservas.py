"""Silver de Espacios: espacios afectados (casos 2 y 4) y reservas (casos 5 y 6).
Unitarios sobre los aplicar_*; no tocan Azure."""
import pandas as pd

from espacios.bp_espacios_silver import (
    aplicar_evento_afectado,
    aplicar_evento_reserva,
    dataframe_vacio_afectados,
    dataframe_vacio_reservas,
)


def _ev(event_type, **data):
    return {"metadata": {"eventType": f"com.citypass.espacios.{event_type}"}, "data": data}


def _afectado(afectacion_id, causa_tipo, event_type="EspacioPublicoAfectado.v2"):
    return _ev(event_type, afectacionId=afectacion_id, espacioId="ESP-1", categoria="PLAZA",
               latitud=-34.5875, longitud=-58.4205, tipoAfectacion="USO_PARCIAL",
               afectacionDesde="2026-11-16T17:28:39.672Z", afectacionHastaEstimada=None,
               causaId="R-1", causaTipo=causa_tipo, correlationId="x",
               occurredAt="2026-11-16T17:28:39.672Z")


def test_afectado_v2_se_carga_con_causa_y_barrio():
    df = aplicar_evento_afectado(dataframe_vacio_afectados(), _afectado("AF-1", "RECLAMO"))

    assert len(df) == 1
    fila = df.iloc[0]
    assert fila["causaTipo"] == "RECLAMO"
    assert fila["espacioId"] == "ESP-1"
    assert fila["barrio"] is not None
    assert fila["occurredAt"] == pd.Timestamp("2026-11-16T17:28:39.672Z")


def test_afectado_repetido_no_se_duplica():
    df = dataframe_vacio_afectados()
    df = aplicar_evento_afectado(df, _afectado("AF-1", "INCIDENTE_VIAL"))
    df = aplicar_evento_afectado(df, _afectado("AF-1", "INCIDENTE_VIAL", event_type="EspacioPublicoAfectado"))
    assert len(df) == 1


def test_afectado_ignora_otros_eventos():
    assert aplicar_evento_afectado(dataframe_vacio_afectados(), _ev("ActividadCreada", actividadId="A")) is None


def test_reserva_creada_arranca_pendiente_y_se_confirma():
    df = dataframe_vacio_reservas()
    df = aplicar_evento_reserva(df, _ev("ReservaCreada", reservaId="RS-1", tipoReserva="ESPACIO",
                                        espacioId="ESP-1", cantidadPersonas=10,
                                        latitud=-34.5875, longitud=-58.4205,
                                        fechaHora="2026-11-17T21:00:00.000Z",
                                        createdAt="2026-11-17T16:42:22.113Z"))
    assert df.iloc[0]["estado_actual"] == "PENDIENTE"

    df = aplicar_evento_reserva(df, _ev("ReservaActualizada", reservaId="RS-1", estadoAnterior="PENDIENTE",
                                        estadoNuevo="CONFIRMADA", updatedAt="2026-11-17T16:42:24.443Z"))
    fila = df.iloc[0]
    assert fila["estado_actual"] == "CONFIRMADA"
    assert fila["tipoReserva"] == "ESPACIO"
    assert len(df) == 1


def test_actualizacion_vieja_no_pisa_una_mas_nueva():
    df = dataframe_vacio_reservas()
    df = aplicar_evento_reserva(df, _ev("ReservaActualizada", reservaId="RS-1", estadoNuevo="CANCELADA",
                                        updatedAt="2026-11-17T18:00:00Z"))
    df = aplicar_evento_reserva(df, _ev("ReservaActualizada", reservaId="RS-1", estadoNuevo="CONFIRMADA",
                                        updatedAt="2026-11-17T17:00:00Z"))
    assert df.iloc[0]["estado_actual"] == "CANCELADA"


def test_actualizada_antes_que_creada_no_vuelve_a_pendiente():
    df = dataframe_vacio_reservas()
    df = aplicar_evento_reserva(df, _ev("ReservaActualizada", reservaId="RS-1", estadoNuevo="CONFIRMADA",
                                        updatedAt="2026-11-17T17:00:00Z"))
    df = aplicar_evento_reserva(df, _ev("ReservaCreada", reservaId="RS-1", tipoReserva="EVENTO"))
    assert df.iloc[0]["estado_actual"] == "CONFIRMADA"
    assert df.iloc[0]["tipoReserva"] == "EVENTO"


def test_reserva_afectada_por_emergencia_caso_5():
    df = dataframe_vacio_reservas()
    df = aplicar_evento_reserva(df, _ev("ReservaAfectada", reservaId="RS-9", correlationId="E-1",
                                        emergenciaId="E-1", espacioId="ESP-1", tipoReserva="EVENTO",
                                        accion="SUSPENDIDA", afectadaEn="2026-11-16T16:59:07.264Z"))
    fila = df.iloc[0]
    assert bool(fila["afectada"]) is True
    assert fila["emergenciaIdAfectacion"] == "E-1"
    assert fila["accionAfectacion"] == "SUSPENDIDA"


def test_reserva_ignora_otros_eventos():
    assert aplicar_evento_reserva(dataframe_vacio_reservas(), _ev("ActividadCreada", actividadId="A")) is None


# --- Gold ------------------------------------------------------------------

from datetime import datetime, timezone  # noqa: E402

from espacios.bp_espacios_gold import calcular_resumen_afectados, calcular_resumen_reservas  # noqa: E402


def test_gold_afectados_cuenta_por_causa_y_espacios_distintos():
    df = dataframe_vacio_afectados()
    df = aplicar_evento_afectado(df, _afectado("AF-1", "RECLAMO"))
    df = aplicar_evento_afectado(df, _afectado("AF-2", "RECLAMO"))
    df = aplicar_evento_afectado(df, _afectado("AF-3", "INCIDENTE_VIAL"))

    resumen = calcular_resumen_afectados(df, fecha_snapshot=datetime(2026, 11, 18, tzinfo=timezone.utc))

    reclamo = resumen[resumen["causaTipo"] == "RECLAMO"].iloc[0]
    assert reclamo["cantidadAfectaciones"] == 2
    assert reclamo["espaciosDistintos"] == 1
    assert resumen["cantidadAfectaciones"].sum() == 3


def test_gold_reservas_separa_tipo_estado_y_afectadas():
    df = dataframe_vacio_reservas()
    df = aplicar_evento_reserva(df, _ev("ReservaCreada", reservaId="RS-1", tipoReserva="ESPACIO", cantidadPersonas=10))
    df = aplicar_evento_reserva(df, _ev("ReservaActualizada", reservaId="RS-1", estadoNuevo="CONFIRMADA",
                                        updatedAt="2026-11-17T17:00:00Z"))
    df = aplicar_evento_reserva(df, _ev("ReservaCreada", reservaId="RS-2", tipoReserva="EVENTO", cantidadPersonas=1))
    df = aplicar_evento_reserva(df, _ev("ReservaAfectada", reservaId="RS-2", emergenciaId="E-1", accion="SUSPENDIDA"))

    resumen = calcular_resumen_reservas(df, fecha_snapshot=datetime(2026, 11, 18, tzinfo=timezone.utc))

    ciudadanas = resumen[(resumen["tipoReserva"] == "ESPACIO") & (resumen["estado_actual"] == "CONFIRMADA")]
    assert ciudadanas["cantidadReservas"].sum() == 1
    assert ciudadanas["cantidadPersonas"].sum() == 10
    assert resumen.loc[resumen["afectada"], "cantidadReservas"].sum() == 1
