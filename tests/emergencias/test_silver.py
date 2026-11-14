"""Silver de Emergencias: se parece a Reclamos, pero con 2 diferencias clave
que probamos ac: la deduplicacion es por columna de fecha puntual (no por
lista de eventId), y la fecha de una transicion prioriza resueltoAt sobre
updatedAt cuando ambas vienen.
"""
from datetime import datetime, timezone

import pandas as pd

from emergencias.bp_emergencias_silver import aplicar_evento, dataframe_vacio


def _evento_creada(
    emergencia_id: str, creado_en: datetime,
    causa_id: str | None = None, causa_tipo: str | None = None,
    latitud: float | None = None, longitud: float | None = None,
) -> dict:
    return {
        "metadata": {"eventType": "com.citypass.emergencias.EmergenciaCreada"},
        "data": {
            "emergenciaId": emergencia_id,
            "correlationId": emergencia_id,  # el contrato lo garantiza igual al emergenciaId
            "prioridad": "MEDIA",
            "createdAt": creado_en.isoformat(),
            "causaId": causa_id,
            "causaTipo": causa_tipo,
            "ubicacion": {"latitud": latitud, "longitud": longitud},
        },
    }


def _evento_actualizada(
    emergencia_id: str, estado_anterior: str, estado_nuevo: str,
    updated_at: datetime, resuelto_at: datetime | None = None,
) -> dict:
    data = {
        "emergenciaId": emergencia_id,
        "correlationId": emergencia_id,  # el contrato lo garantiza igual al emergenciaId
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


def test_emergencia_creada_con_causa_carga_causaid_y_causatipo():
    df = dataframe_vacio()
    df = aplicar_evento(
        df,
        _evento_creada(
            "E-004", datetime(2026, 9, 1, tzinfo=timezone.utc),
            causa_id="EVT-001", causa_tipo="EVENTO_PUBLICO",
        ),
    )

    fila = df[df["emergenciaId"] == "E-004"].iloc[0]
    assert fila["causaId"] == "EVT-001"
    assert fila["causaTipo"] == "EVENTO_PUBLICO"


def test_emergencia_creada_organica_queda_con_causa_nula():
    df = dataframe_vacio()
    df = aplicar_evento(df, _evento_creada("E-005", datetime(2026, 9, 1, tzinfo=timezone.utc)))

    fila = df[df["emergenciaId"] == "E-005"].iloc[0]
    assert pd.isna(fila["causaId"])
    assert pd.isna(fila["causaTipo"])


def test_emergencia_creada_toma_latitud_longitud_de_ubicacion_anidada():
    df = dataframe_vacio()
    df = aplicar_evento(
        df,
        _evento_creada(
            "E-006", datetime(2026, 9, 1, tzinfo=timezone.utc),
            latitud=-34.6, longitud=-58.4,
        ),
    )

    fila = df[df["emergenciaId"] == "E-006"].iloc[0]
    assert fila["latitud"] == -34.6
    assert fila["longitud"] == -58.4


def test_emergencia_creada_con_latitud_no_numerica_queda_nula():
    df = dataframe_vacio()
    df = aplicar_evento(
        df,
        _evento_creada(
            "E-007", datetime(2026, 9, 1, tzinfo=timezone.utc),
            latitud="RUxDAStE...no-es-un-numero", longitud=None,
        ),
    )

    fila = df[df["emergenciaId"] == "E-007"].iloc[0]
    assert pd.isna(fila["latitud"])
    assert pd.isna(fila["longitud"])


def test_emergencia_cancelada_marca_columna_cancelada_y_estado_actual():
    df = dataframe_vacio()
    df = aplicar_evento(df, _evento_creada("E-008", datetime(2026, 9, 1, tzinfo=timezone.utc)))

    df = aplicar_evento(
        df,
        _evento_actualizada("E-008", "PENDIENTE", "CANCELADA", datetime(2026, 9, 1, 1, 0, tzinfo=timezone.utc)),
    )

    fila = df[df["emergenciaId"] == "E-008"].iloc[0]
    assert fila["cancelada"] == True
    assert fila["descartada"] == False
    assert fila["estado_actual"] == "CANCELADA"


def test_actualizada_se_relaciona_por_correlationid_no_por_emergenciaid():
    df = dataframe_vacio()
    df = aplicar_evento(df, _evento_creada("E-009", datetime(2026, 9, 1, tzinfo=timezone.utc)))

    evento_actualizada = _evento_actualizada(
        "E-009", "PENDIENTE", "VALIDADA", datetime(2026, 9, 1, 1, 0, tzinfo=timezone.utc)
    )
    # emergenciaId "distinto" en la Actualizada (no deberia pasar en la
    # practica, pero confirma que lo que realmente se usa para relacionar
    # es correlationId, no emergenciaId).
    evento_actualizada["data"]["emergenciaId"] = "OTRO-ID"

    df = aplicar_evento(df, evento_actualizada)

    assert len(df) == 1
    fila = df.iloc[0]
    assert fila["correlationId"] == "E-009"
    assert fila["estado_actual"] == "VALIDADA"


def test_actualizada_antes_que_creada_captura_emergenciaid_igual():
    df = dataframe_vacio()
    df = aplicar_evento(
        df, _evento_actualizada("E-010", "PENDIENTE", "VALIDADA", datetime(2026, 9, 1, 1, 0, tzinfo=timezone.utc))
    )

    fila = df[df["correlationId"] == "E-010"].iloc[0]
    assert fila["emergenciaId"] == "E-010"
