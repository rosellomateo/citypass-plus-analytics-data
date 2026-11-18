"""Gold de Residuos (intervenciones): el resumen cuenta intervenciones y
afectadas por estado_actual/causaTipo, a partir de una tabla de Silver
armada a mano. No lee ni escribe ningun archivo, no se conecta a Azure.
"""
from datetime import datetime, timezone

import pandas as pd

from residuos.bp_residuos_gold import calcular_resumen


def _fila(intervencion_id, estado_actual, causa_tipo, afectada):
    return {
        "intervencionId": intervencion_id,
        "estado_actual": estado_actual,
        "causaTipo": causa_tipo,
        "afectada": afectada,
    }


def test_calcular_resumen_cuenta_por_estado_y_causatipo():
    filas = [
        _fila("I-001", "AFECTADA", "EVENTO_PUBLICO", True),
        _fila("I-002", "AFECTADA", "EVENTO_PUBLICO", True),
        _fila("I-003", "PROGRAMADA", "RESERVA", False),
    ]
    df_silver = pd.DataFrame(filas)

    resumen = calcular_resumen(df_silver, fecha_snapshot=datetime(2026, 9, 5, tzinfo=timezone.utc))

    assert len(resumen) == 2

    fila_afectada = resumen[resumen["estado_actual"] == "AFECTADA"].iloc[0]
    assert fila_afectada["cantidadIntervenciones"] == 2
    assert fila_afectada["cantidadAfectadas"] == 2

    fila_programada = resumen[resumen["estado_actual"] == "PROGRAMADA"].iloc[0]
    assert fila_programada["cantidadIntervenciones"] == 1
    assert fila_programada["cantidadAfectadas"] == 0


def test_calcular_resumen_no_pierde_filas_con_causatipo_nulo():
    # Intervenciones organicas (sin causa externa) tienen causaTipo null.
    filas = [
        _fila("I-004", "PROGRAMADA", None, False),
        _fila("I-005", "PROGRAMADA", None, False),
    ]
    df_silver = pd.DataFrame(filas)

    resumen = calcular_resumen(df_silver, fecha_snapshot=datetime(2026, 9, 5, tzinfo=timezone.utc))

    assert len(resumen) == 1
    assert resumen.iloc[0]["cantidadIntervenciones"] == 2
    assert pd.isna(resumen.iloc[0]["causaProgramacion"])


def test_resumen_separa_causa_de_programacion_y_de_afectacion():
    """Casos 1/4 (causaTipoAfectacion) y 2/3/6 (causaProgramacion) salen de la misma tabla."""
    df_silver = pd.DataFrame([
        {"intervencionId": "I-1", "estado_actual": "AFECTADA", "causaTipo": "RECLAMO",
         "causaTipoAfectacion": "EVENTO_CLIMATICO", "barrio": "Palermo", "afectada": True,
         "cancelada": False, "completada": False, "camionId": "C1", "emergenciaId": None, "alertaId": None},
        {"intervencionId": "I-2", "estado_actual": "CANCELADA", "causaTipo": None,
         "causaTipoAfectacion": "INCIDENTE_VIAL", "barrio": "Palermo", "afectada": False,
         "cancelada": True, "completada": False, "camionId": "C2", "emergenciaId": None, "alertaId": None},
    ])

    resumen = calcular_resumen(df_silver, fecha_snapshot=datetime(2026, 9, 5, tzinfo=timezone.utc))

    clima = resumen[resumen["causaTipoAfectacion"] == "EVENTO_CLIMATICO"].iloc[0]
    assert clima["causaProgramacion"] == "RECLAMO"
    assert clima["cantidadAfectadas"] == 1
    incidente = resumen[resumen["causaTipoAfectacion"] == "INCIDENTE_VIAL"].iloc[0]
    assert incidente["cantidadCanceladas"] == 1
    assert set(resumen["barrio"]) == {"Palermo"}


def test_origen_programacion_distingue_emergencia_de_alerta():
    """Caso 5: programadas por una emergencia vs. por una alerta normal."""
    df_silver = pd.DataFrame([
        {"intervencionId": "I-1", "estado_actual": "PROGRAMADA", "emergenciaId": "E-1", "alertaId": None},
        {"intervencionId": "I-2", "estado_actual": "PROGRAMADA", "emergenciaId": None, "alertaId": "A-1"},
        {"intervencionId": "I-3", "estado_actual": "PROGRAMADA", "emergenciaId": None, "alertaId": None},
    ])

    resumen = calcular_resumen(df_silver, fecha_snapshot=datetime(2026, 9, 5, tzinfo=timezone.utc))

    por_origen = resumen.set_index("origenProgramacion")["cantidadIntervenciones"].to_dict()
    assert por_origen == {"EMERGENCIA": 1, "ALERTA": 1, "NINGUNO": 1}
