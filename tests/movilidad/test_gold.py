"""Gold de Movilidad: combina viajes.parquet y estaciones_afectadas.parquet
en una sola tabla agrupada por causaTipo (el enum que comparten los dos
eventos), para responder las 6 metricas de CONTRATO_MOVILIDAD.md seccion 5
sin necesitar tablas separadas."""
from datetime import datetime, timezone

import pandas as pd

from movilidad.bp_movilidad_gold import calcular_resumen


def _fila_estacion(
    afectacion_id, estacion_id, causa_tipo, tipo_afectacion="BLOQUEADA",
    afectacion_desde=None, occurred_at=None, restaurada=False,
):
    desde = afectacion_desde or datetime(2026, 11, 12, tzinfo=timezone.utc)
    ocurrida = occurred_at or datetime(2026, 11, 12, tzinfo=timezone.utc)
    return {
        "afectacionId": afectacion_id, "estacionId": estacion_id, "causaTipo": causa_tipo,
        "tipoAfectacion": tipo_afectacion, "afectacionDesde": desde, "occurredAt": ocurrida,
        "restaurada": restaurada,
    }


def _fila_viaje(viaje_id, causa_tipo):
    return {"viajeId": viaje_id, "causaTipo": causa_tipo}


def _df_estaciones(filas):
    df = pd.DataFrame(filas)
    df["afectacionDesde"] = pd.to_datetime(df["afectacionDesde"], utc=True)
    df["occurredAt"] = pd.to_datetime(df["occurredAt"], utc=True)
    return df


def test_caso_1_bloqueadas_por_clima():
    df_estaciones = _df_estaciones([
        _fila_estacion("AF-1", "EST-01", "EVENTO_CLIMATICO", tipo_afectacion="BLOQUEADA"),
        _fila_estacion("AF-2", "EST-02", "EVENTO_CLIMATICO", tipo_afectacion="CAPACIDAD_REDUCIDA"),
    ])
    df_viajes = pd.DataFrame([], columns=["viajeId", "causaTipo"])

    resumen = calcular_resumen(df_viajes, df_estaciones, fecha_snapshot=datetime(2026, 9, 5, tzinfo=timezone.utc))

    fila = resumen[resumen["causaTipo"] == "EVENTO_CLIMATICO"].iloc[0]
    assert fila["cantidadBloqueadas"] == 1
    assert fila["cantidadAfectaciones"] == 2


def test_caso_2_y_4_afectadas_por_reclamo_e_incidente():
    df_estaciones = _df_estaciones([
        _fila_estacion("AF-3", "EST-03", "RECLAMO"),
        _fila_estacion("AF-4", "EST-04", "INCIDENTE_VIAL"),
        _fila_estacion("AF-5", "EST-04", "INCIDENTE_VIAL"),  # misma estacion, 2da afectacion
    ])
    df_viajes = pd.DataFrame([], columns=["viajeId", "causaTipo"])

    resumen = calcular_resumen(df_viajes, df_estaciones, fecha_snapshot=datetime(2026, 9, 5, tzinfo=timezone.utc))

    assert resumen[resumen["causaTipo"] == "RECLAMO"].iloc[0]["cantidadAfectaciones"] == 1
    incidente = resumen[resumen["causaTipo"] == "INCIDENTE_VIAL"].iloc[0]
    assert incidente["cantidadAfectaciones"] == 2
    assert incidente["estacionesAfectadasDistintas"] == 1  # misma estacion las 2 veces


def test_caso_3_afectacion_preventiva():
    publicada_antes = datetime(2026, 11, 10, tzinfo=timezone.utc)
    df_estaciones = _df_estaciones([
        # Preventiva: afectacionDesde es futuro respecto de occurredAt.
        _fila_estacion(
            "AF-6", "EST-06", "EVENTO_PUBLICO",
            afectacion_desde=datetime(2026, 11, 15, tzinfo=timezone.utc), occurred_at=publicada_antes,
        ),
        # No preventiva: ya esta bloqueada al momento de publicarse.
        _fila_estacion(
            "AF-7", "EST-07", "EVENTO_PUBLICO",
            afectacion_desde=publicada_antes, occurred_at=publicada_antes,
        ),
    ])
    df_viajes = pd.DataFrame([], columns=["viajeId", "causaTipo"])

    resumen = calcular_resumen(df_viajes, df_estaciones, fecha_snapshot=datetime(2026, 9, 5, tzinfo=timezone.utc))

    fila = resumen[resumen["causaTipo"] == "EVENTO_PUBLICO"].iloc[0]
    assert fila["cantidadPreventivas"] == 1
    assert fila["cantidadAfectaciones"] == 2


def test_caso_5_viajes_interrumpidos_vs_normales():
    df_estaciones = pd.DataFrame([], columns=[
        "afectacionId", "estacionId", "causaTipo", "tipoAfectacion",
        "afectacionDesde", "occurredAt", "restaurada",
    ])
    df_viajes = pd.DataFrame([
        _fila_viaje("V-1", "INCIDENTE_VIAL"),
        _fila_viaje("V-2", None),
        _fila_viaje("V-3", None),
    ])

    resumen = calcular_resumen(df_viajes, df_estaciones, fecha_snapshot=datetime(2026, 9, 5, tzinfo=timezone.utc))

    interrumpidos = resumen[resumen["causaTipo"] == "INCIDENTE_VIAL"].iloc[0]
    assert interrumpidos["cantidadViajesInterrumpidos"] == 1

    normales = resumen[resumen["causaTipo"] == "NORMAL"].iloc[0]
    assert normales["cantidadViajesInterrumpidos"] == 2
    # Una fila NORMAL no corresponde a ninguna afectacion de estacion.
    assert normales["cantidadAfectaciones"] == 0
    assert normales["estacionesAfectadasDistintas"] == 0


def test_caso_6_afectadas_por_reserva():
    df_estaciones = _df_estaciones([_fila_estacion("AF-8", "EST-08", "RESERVA")])
    df_viajes = pd.DataFrame([], columns=["viajeId", "causaTipo"])

    resumen = calcular_resumen(df_viajes, df_estaciones, fecha_snapshot=datetime(2026, 9, 5, tzinfo=timezone.utc))

    assert resumen[resumen["causaTipo"] == "RESERVA"].iloc[0]["cantidadAfectaciones"] == 1


def test_restauradas_se_cuentan():
    df_estaciones = _df_estaciones([
        _fila_estacion("AF-9", "EST-09", "EMERGENCIA", restaurada=True),
        _fila_estacion("AF-10", "EST-10", "EMERGENCIA", restaurada=False),
    ])
    df_viajes = pd.DataFrame([], columns=["viajeId", "causaTipo"])

    resumen = calcular_resumen(df_viajes, df_estaciones, fecha_snapshot=datetime(2026, 9, 5, tzinfo=timezone.utc))

    assert resumen[resumen["causaTipo"] == "EMERGENCIA"].iloc[0]["cantidadRestauradas"] == 1
