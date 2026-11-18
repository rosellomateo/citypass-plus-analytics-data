"""Silver de Movilidad: dos flujos independientes.

ViajeFinalizado ya trae el viaje completo (inicio + fin) en un solo evento,
asi que no hay estado "en curso" ni join -- la fila queda completa de una.

EstacionUrbanaAfectada/EstacionUrbanaRestaurada si son dos eventos que se
juntan por afectacionId, igual que Residuos/Espacios."""
from datetime import datetime, timezone

import pandas as pd

from movilidad.bp_movilidad_silver import (
    aplicar_evento,
    aplicar_evento_estacion,
    dataframe_vacio,
    dataframe_vacio_estaciones,
)


def _epoch_iso(momento: datetime) -> str:
    return momento.isoformat()


def _evento_viaje_finalizado(
    viaje_id: str, inicio: datetime, fin: datetime,
    causa_id: str | None = None, causa_tipo: str | None = None,
) -> dict:
    return {
        "metadata": {"eventType": "com.citypass.movilidad.ViajeFinalizado"},
        "data": {
            "viajeId": viaje_id,
            "correlationId": f"viaje-{viaje_id}",
            "estacionInicio": "EST-007",
            "latitudInicio": -34.6066,
            "longitudInicio": -58.4357,
            "horaInicio": _epoch_iso(inicio),
            "estacionFin": "EST-010",
            "latitudFin": -34.5622,
            "longitudFin": -58.4522,
            "horaFinalizacion": _epoch_iso(fin),
            "causaId": causa_id,
            "causaTipo": causa_tipo,
            "createdAt": _epoch_iso(fin),
        },
    }


def test_viaje_finalizado_queda_completo_de_una():
    df = dataframe_vacio()
    inicio = datetime(2026, 9, 1, 8, 0, tzinfo=timezone.utc)
    fin = datetime(2026, 9, 1, 8, 25, tzinfo=timezone.utc)

    df = aplicar_evento(df, _evento_viaje_finalizado("V-001", inicio, fin))

    assert len(df) == 1
    fila = df.iloc[0]
    assert fila["viajeId"] == "V-001"
    assert fila["duracion_minutos"] == 25.0
    assert fila["causaTipo"] is None


def test_viaje_finalizado_con_causa_incidente_vial():
    df = dataframe_vacio()
    inicio = datetime(2026, 9, 1, 8, 0, tzinfo=timezone.utc)
    fin = datetime(2026, 9, 1, 8, 10, tzinfo=timezone.utc)

    df = aplicar_evento(
        df, _evento_viaje_finalizado("V-002", inicio, fin, causa_id="inc-1", causa_tipo="INCIDENTE_VIAL"),
    )

    fila = df[df["viajeId"] == "V-002"].iloc[0]
    assert fila["causaTipo"] == "INCIDENTE_VIAL"


def test_viajefinalizado_duplicado_no_pisa_datos():
    df = dataframe_vacio()
    inicio = datetime(2026, 9, 1, 8, 0, tzinfo=timezone.utc)
    fin_real = datetime(2026, 9, 1, 8, 25, tzinfo=timezone.utc)
    df = aplicar_evento(df, _evento_viaje_finalizado("V-003", inicio, fin_real))

    # Reentrega del mismo evento con otra hora -- ya tiene horaFinalizacion, se ignora.
    df = aplicar_evento(df, _evento_viaje_finalizado("V-003", inicio, datetime(2026, 9, 1, 10, 0, tzinfo=timezone.utc)))

    fila = df[df["viajeId"] == "V-003"].iloc[0]
    assert fila["horaFinalizacion"] == pd.Timestamp(fin_real)
    assert fila["duracion_minutos"] == 25.0


def test_evento_no_reconocido_devuelve_none():
    df = dataframe_vacio()
    evento = {"metadata": {"eventType": "com.citypass.movilidad.EmergenciaBicicleta"}, "data": {}}

    assert aplicar_evento(df, evento) is None


# --- Estaciones afectadas --------------------------------------------------

def _evento_estacion_afectada(
    afectacion_id: str, estacion_id: str = "EST-042",
    tipo_afectacion: str = "BLOQUEADA", causa_tipo: str = "EVENTO_CLIMATICO",
    desde: datetime = datetime(2026, 11, 15, 16, 0, tzinfo=timezone.utc),
) -> dict:
    return {
        "metadata": {"eventType": "com.citypass.movilidad.EstacionUrbanaAfectada"},
        "data": {
            "afectacionId": afectacion_id,
            "estacionId": estacion_id,
            "latitud": -34.5711,
            "longitud": -58.4233,
            "tipoAfectacion": tipo_afectacion,
            "afectacionDesde": _epoch_iso(desde),
            "afectacionHastaEstimada": None,
            "causaId": "causa-1",
            "causaTipo": causa_tipo,
            "moduloOrigen": "MOVILIDAD",
            "correlationId": f"c-{afectacion_id}",
            "occurredAt": _epoch_iso(desde),
        },
    }


def _evento_estacion_restaurada(afectacion_id: str, estacion_id: str, restaurada_en: datetime) -> dict:
    return {
        "metadata": {"eventType": "com.citypass.movilidad.EstacionUrbanaRestaurada"},
        "data": {
            "afectacionId": afectacion_id,
            "estacionId": estacion_id,
            "correlationId": f"c-{afectacion_id}",
            "restauradaAt": _epoch_iso(restaurada_en),
        },
    }


def test_estacion_afectada_nueva_queda_sin_restaurar():
    df = dataframe_vacio_estaciones()
    df = aplicar_evento_estacion(df, _evento_estacion_afectada("AF-001"))

    assert len(df) == 1
    fila = df.iloc[0]
    assert fila["afectacionId"] == "AF-001"
    assert fila["estacionId"] == "EST-042"
    assert fila["tipoAfectacion"] == "BLOQUEADA"
    assert fila["restaurada"] == False  # noqa: E712
    assert pd.isna(fila["restauradaAt"])


def test_estacion_restaurada_cierra_la_afectacion():
    df = dataframe_vacio_estaciones()
    df = aplicar_evento_estacion(df, _evento_estacion_afectada("AF-002"))

    restaurada_en = datetime(2026, 11, 16, 2, 0, tzinfo=timezone.utc)
    df = aplicar_evento_estacion(df, _evento_estacion_restaurada("AF-002", "EST-042", restaurada_en))

    fila = df[df["afectacionId"] == "AF-002"].iloc[0]
    assert fila["restaurada"] == True  # noqa: E712
    assert fila["restauradaAt"] == pd.Timestamp(restaurada_en)


def test_estacion_restaurada_antes_que_afectada_crea_placeholder():
    df = dataframe_vacio_estaciones()
    restaurada_en = datetime(2026, 11, 16, 2, 0, tzinfo=timezone.utc)

    df = aplicar_evento_estacion(df, _evento_estacion_restaurada("AF-003", "EST-099", restaurada_en))

    assert len(df) == 1
    fila = df.iloc[0]
    assert fila["afectacionId"] == "AF-003"
    assert fila["estacionId"] == "EST-099"
    assert fila["restaurada"] == True  # noqa: E712
    assert pd.isna(fila["tipoAfectacion"])

    # Llega despues la afectacion -- no debe pisar que ya esta restaurada.
    df = aplicar_evento_estacion(df, _evento_estacion_afectada("AF-003", estacion_id="EST-099"))
    fila = df[df["afectacionId"] == "AF-003"].iloc[0]
    assert fila["restaurada"] == True  # noqa: E712
    assert fila["tipoAfectacion"] == "BLOQUEADA"


def test_estacion_afectada_duplicada_no_pisa_datos():
    df = dataframe_vacio_estaciones()
    df = aplicar_evento_estacion(df, _evento_estacion_afectada("AF-004", tipo_afectacion="BLOQUEADA"))
    df = aplicar_evento_estacion(df, _evento_estacion_afectada("AF-004", tipo_afectacion="FUERA_DE_SERVICIO"))

    fila = df[df["afectacionId"] == "AF-004"].iloc[0]
    assert fila["tipoAfectacion"] == "BLOQUEADA"


def test_estacion_restaurada_duplicada_no_pisa_datos():
    df = dataframe_vacio_estaciones()
    df = aplicar_evento_estacion(df, _evento_estacion_afectada("AF-005"))
    primera_restauracion = datetime(2026, 11, 16, 2, 0, tzinfo=timezone.utc)
    df = aplicar_evento_estacion(df, _evento_estacion_restaurada("AF-005", "EST-042", primera_restauracion))

    # Reentrega con otra hora -- ya esta restaurada, se ignora.
    df = aplicar_evento_estacion(
        df, _evento_estacion_restaurada("AF-005", "EST-042", datetime(2026, 11, 17, 0, 0, tzinfo=timezone.utc)),
    )

    fila = df[df["afectacionId"] == "AF-005"].iloc[0]
    assert fila["restauradaAt"] == pd.Timestamp(primera_restauracion)


def test_evento_de_viaje_no_lo_procesa_el_dispatcher_de_estaciones():
    df = dataframe_vacio_estaciones()
    evento = {"metadata": {"eventType": "com.citypass.movilidad.ViajeFinalizado"}, "data": {"viajeId": "V-900"}}

    assert aplicar_evento_estacion(df, evento) is None


def test_fechas_con_nanosegundos_se_recortan_a_microsegundos():
    """Movilidad manda ".841938634Z": la columna es datetime64[us] y asignar
    un Timestamp con nanosegundos puede ser un cast con perdida."""
    from movilidad.bp_movilidad_silver import _iso_a_datetime
    ts = _iso_a_datetime("2026-11-17T16:17:11.841938634Z")
    assert ts.unit == "us"
    assert ts.nanosecond == 0
    assert ts.microsecond == 841938


def test_tabla_de_estaciones_con_fechas_object_se_normaliza():
    """El parquet de estaciones lo escribio una corrida local y sus fechas
    quedaron como object: al leerlo se pasan a datetime64[us, UTC]."""
    from io import BytesIO
    from unittest.mock import MagicMock
    import pandas as pd
    from movilidad.bp_movilidad_silver import leer_tabla_estaciones, COLUMNAS_ESTACION
    vieja = pd.DataFrame([{c: None for c in COLUMNAS_ESTACION}])
    vieja["afectacionId"] = "AF-1"
    vieja["occurredAt"] = pd.Series([pd.Timestamp("2026-11-15T17:40:11.408953Z")], dtype="object")
    vieja["restaurada"] = None
    buf = BytesIO(); vieja.to_parquet(buf, index=False)
    blob = MagicMock(); blob.download_blob.return_value.readall.return_value = buf.getvalue()

    df = leer_tabla_estaciones(blob)

    assert str(df["occurredAt"].dtype) == "datetime64[us, UTC]"
    assert df["restaurada"].dtype == bool
