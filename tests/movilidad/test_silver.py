"""Silver de Movilidad: la particularidad de este dominio es que un viaje se
arma con DOS eventos separados (ViajeIniciado / ViajeTerminado) que pueden
llegar en cualquier orden, y algunos viajes se quedan "en curso" a proposito
(sin ViajeTerminado)."""
from datetime import datetime, timezone

import pandas as pd

from movilidad.bp_movilidad_silver import aplicar_evento, dataframe_vacio


def _evento_iniciado(viaje_id: str, hora_inicio: datetime) -> dict:
    return {
        "metadata": {"eventType": "com.citypass.movilidad.ViajeIniciado"},
        "data": {
            "id": viaje_id,
            "estacion": "liniers-02",
            "barrioInicio": "Liniers",
            "latitud": -34.64,
            "longitud": -58.52,
            "horaInicio": hora_inicio.isoformat(),
        },
    }


def _evento_terminado(viaje_id: str, hora_fin: datetime) -> dict:
    return {
        "metadata": {"eventType": "com.citypass.movilidad.ViajeTerminado"},
        "data": {
            "id": viaje_id,
            "estacion": "recoleta-04",
            "latitud": -34.58,
            "longitud": -58.40,
            "horaFinalizacion": hora_fin.isoformat(),
        },
    }


def test_viaje_sin_viajeterminado_queda_en_curso():
    df = dataframe_vacio()
    df = aplicar_evento(df, _evento_iniciado("V-001", datetime(2026, 9, 1, 8, 0, tzinfo=timezone.utc)))

    fila = df.iloc[0]
    assert fila["en_curso"] is True or fila["en_curso"] == True  # noqa: E712
    assert pd.isna(fila["duracion_minutos"])


def test_viaje_completo_calcula_duracion_en_minutos():
    df = dataframe_vacio()
    inicio = datetime(2026, 9, 1, 8, 0, tzinfo=timezone.utc)
    fin = datetime(2026, 9, 1, 8, 25, tzinfo=timezone.utc)

    df = aplicar_evento(df, _evento_iniciado("V-002", inicio))
    df = aplicar_evento(df, _evento_terminado("V-002", fin))

    fila = df.iloc[0]
    assert fila["en_curso"] == False  # noqa: E712
    assert fila["duracion_minutos"] == 25.0


def test_viajeterminado_que_llega_antes_crea_fila_placeholder_y_se_completa_despues():
    df = dataframe_vacio()
    fin = datetime(2026, 9, 1, 8, 25, tzinfo=timezone.utc)

    # Llega primero (en el tiempo real) el evento de fin, sin el de inicio.
    df = aplicar_evento(df, _evento_terminado("V-003", fin))
    assert len(df) == 1
    fila = df.iloc[0]
    # en_curso solo mira si FALTA el evento de cierre (ya llego, asi que da
    # False) -- no es un indicador de "tiene todos sus datos". Como todavia
    # no llego el inicio, la duracion no se puede calcular.
    assert fila["en_curso"] == False  # noqa: E712
    assert pd.isna(fila["duracion_minutos"])

    # Llega despues el evento de inicio.
    inicio = datetime(2026, 9, 1, 8, 0, tzinfo=timezone.utc)
    df = aplicar_evento(df, _evento_iniciado("V-003", inicio))

    fila = df.iloc[0]
    assert fila["en_curso"] == False  # noqa: E712
    assert fila["duracion_minutos"] == 25.0


def test_viajeiniciado_duplicado_no_pisa_datos():
    df = dataframe_vacio()
    inicio_real = datetime(2026, 9, 1, 8, 0, tzinfo=timezone.utc)
    df = aplicar_evento(df, _evento_iniciado("V-004", inicio_real))

    # Reentrega del ViajeIniciado (ej. reintento de red) con otra hora --
    # como el viaje ya tiene horaInicio, se debe ignorar.
    df = aplicar_evento(df, _evento_iniciado("V-004", datetime(2026, 9, 1, 9, 30, tzinfo=timezone.utc)))

    fila = df[df["viajeId"] == "V-004"].iloc[0]
    assert fila["horaInicio"] == pd.Timestamp(inicio_real)


def test_viajeterminado_duplicado_no_pisa_datos():
    df = dataframe_vacio()
    inicio = datetime(2026, 9, 1, 8, 0, tzinfo=timezone.utc)
    fin_real = datetime(2026, 9, 1, 8, 25, tzinfo=timezone.utc)
    df = aplicar_evento(df, _evento_iniciado("V-005", inicio))
    df = aplicar_evento(df, _evento_terminado("V-005", fin_real))

    # Reentrega del ViajeTerminado con otra hora -- el viaje ya tiene
    # horaFinalizacion, se debe ignorar.
    df = aplicar_evento(df, _evento_terminado("V-005", datetime(2026, 9, 1, 10, 0, tzinfo=timezone.utc)))

    fila = df[df["viajeId"] == "V-005"].iloc[0]
    assert fila["horaFinalizacion"] == pd.Timestamp(fin_real)
    assert fila["duracion_minutos"] == 25.0
