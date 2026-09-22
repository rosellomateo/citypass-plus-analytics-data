"""Silver de Emergencias: se parece a Reclamos, pero con 2 diferencias clave
que probamos ac: la deduplicacion es por columna de fecha puntual (no por
lista de eventId), y la fecha de una transicion prioriza resueltoAt sobre
updatedAt cuando ambas vienen.
"""
from datetime import datetime, timezone

import pandas as pd

from emergencias.bp_emergencias_silver import aplicar_evento, dataframe_vacio


def _evento_creada(emergencia_id: str, creado_en: datetime) -> dict:
    return {
        "metadata": {"eventType": "com.citypass.emergencias.EmergenciaCreada"},
        "data": {
            "emergenciaId": emergencia_id,
            "prioridad": "MEDIA",
            "createdAt": creado_en.isoformat(),
        },
    }


def _evento_actualizada(
    emergencia_id: str, estado_anterior: str, estado_nuevo: str,
    updated_at: datetime, resuelto_at: datetime | None = None,
) -> dict:
    data = {
        "emergenciaId": emergencia_id,
        "estadoAnterior": estado_anterior,
        "estadoNuevo": estado_nuevo,
        "updatedAt": updated_at.isoformat(),
    }
    if resuelto_at is not None:
        data["resueltoAt"] = resuelto_at.isoformat()
    return {"metadata": {"eventType": "com.citypass.emergencias.EmergenciaActualizada"}, "data": data}


def test_emergencia_creada_nueva_crea_fila_en_pendiente():
    df = dataframe_vacio()
    creado_en = datetime(2026, 9, 1, 8, 0, tzinfo=timezone.utc)

    df = aplicar_evento(df, _evento_creada("E-001", creado_en))

    assert len(df) == 1
    fila = df.iloc[0]
    assert fila["emergenciaId"] == "E-001"
    assert fila["estado_actual"] == "PENDIENTE"


def test_actualizada_duplicada_para_el_mismo_estado_se_ignora():
    df = dataframe_vacio()
    df = aplicar_evento(df, _evento_creada("E-002", datetime(2026, 9, 1, tzinfo=timezone.utc)))

    df = aplicar_evento(
        df, _evento_actualizada("E-002", "PENDIENTE", "DESPACHADA", datetime(2026, 9, 1, 9, 0, tzinfo=timezone.utc))
    )
    # Reenvio tardio del mismo estado (DESPACHADA), con otra hora -- debe
    # ignorarse porque ya hay una fecha cargada para ese estado puntual.
    df = aplicar_evento(
        df, _evento_actualizada("E-002", "PENDIENTE", "DESPACHADA", datetime(2026, 9, 1, 9, 30, tzinfo=timezone.utc))
    )

    fila = df[df["emergenciaId"] == "E-002"].iloc[0]
    assert fila["fecha_despachada"] == pd.Timestamp(datetime(2026, 9, 1, 9, 0, tzinfo=timezone.utc))


def test_resuelta_usa_resueltoat_en_vez_de_updatedat_cuando_viene():
    df = dataframe_vacio()
    df = aplicar_evento(df, _evento_creada("E-003", datetime(2026, 9, 1, tzinfo=timezone.utc)))

    fecha_updated = datetime(2026, 9, 3, 12, 0, tzinfo=timezone.utc)
    fecha_resuelto = datetime(2026, 9, 3, 10, 0, tzinfo=timezone.utc)  # mas temprano que updatedAt

    df = aplicar_evento(
        df, _evento_actualizada("E-003", "EN_LUGAR", "RESUELTA", fecha_updated, resuelto_at=fecha_resuelto)
    )

    fila = df[df["emergenciaId"] == "E-003"].iloc[0]
    assert fila["fecha_resuelta"] == pd.Timestamp(fecha_resuelto)
