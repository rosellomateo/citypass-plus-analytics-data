"""Silver de Residuos (intervenciones): se parece a Emergencias, pero con
solo 2 estados (PROGRAMADA -> AFECTADA) y timestamps en formatos distintos
por evento (IntervencionProgramada en epoch millis, IntervencionActualizada
en ISO-8601) -- igual que vienen en el bus real.
"""
from datetime import datetime, timezone

import pandas as pd

from residuos.bp_residuos_silver import aplicar_evento, dataframe_vacio


def _epoch_ms(momento: datetime) -> int:
    return int(momento.timestamp() * 1000)


def _evento_programada(
    intervencion_id: str, programada_en: datetime,
    causa_id: str | None = None, causa_tipo: str | None = None,
    correlation_id: str | None = None, camion_id: str | None = None,
) -> dict:
    return {
        "metadata": {"eventType": "com.citypass.residuos.IntervencionProgramada"},
        "data": {
            "intervencionId": intervencion_id,
            "programadaEn": _epoch_ms(programada_en),
            "occurredAt": _epoch_ms(programada_en),
            "causaId": causa_id,
            "causaTipo": causa_tipo,
            "correlationId": correlation_id,
            "camionId": camion_id,
        },
    }


def _evento_actualizada(
    intervencion_id: str, estado_anterior: str, estado_nuevo: str,
    updated_at: datetime, camion_id: str | None = "QA124ZZ",
) -> dict:
    return {
        "metadata": {"eventType": "com.citypass.residuos.IntervencionActualizada"},
        "data": {
            "intervencionId": intervencion_id,
            "estadoAnterior": estado_anterior,
            "estadoNuevo": estado_nuevo,
            "updatedAt": updated_at.isoformat(),
            "camionId": camion_id,
            "causaId": None,
            "causaTipo": None,
        },
    }


def test_intervencion_programada_nueva_crea_fila_en_programada():
    df = dataframe_vacio()
    programada_en = datetime(2026, 9, 1, 8, 0, tzinfo=timezone.utc)

    df = aplicar_evento(df, _evento_programada("I-001", programada_en))

    assert len(df) == 1
    fila = df.iloc[0]
    assert fila["intervencionId"] == "I-001"
    assert fila["estado_actual"] == "PROGRAMADA"
    assert fila["afectada"] == False  # noqa: E712


def test_actualizada_marca_afectada_y_captura_camionid():
    df = dataframe_vacio()
    df = aplicar_evento(df, _evento_programada("I-002", datetime(2026, 9, 1, tzinfo=timezone.utc)))

    df = aplicar_evento(
        df,
        _evento_actualizada(
            "I-002", "PROGRAMADA", "AFECTADA", datetime(2026, 9, 1, 1, 0, tzinfo=timezone.utc),
            camion_id="QA124ZZ",
        ),
    )

    fila = df[df["intervencionId"] == "I-002"].iloc[0]
    assert fila["estado_actual"] == "AFECTADA"
    assert fila["afectada"] == True  # noqa: E712
    assert fila["camionId"] == "QA124ZZ"
    assert fila["estado_anterior"] == "PROGRAMADA"


def test_actualizada_duplicada_para_el_mismo_estado_se_ignora():
    df = dataframe_vacio()
    df = aplicar_evento(df, _evento_programada("I-003", datetime(2026, 9, 1, tzinfo=timezone.utc)))
    df = aplicar_evento(
        df, _evento_actualizada("I-003", "PROGRAMADA", "AFECTADA", datetime(2026, 9, 1, 9, 0, tzinfo=timezone.utc))
    )
    # Reenvio tardio del mismo estado, con otra hora -- debe ignorarse.
    df = aplicar_evento(
        df, _evento_actualizada("I-003", "PROGRAMADA", "AFECTADA", datetime(2026, 9, 1, 9, 30, tzinfo=timezone.utc))
    )

    fila = df[df["intervencionId"] == "I-003"].iloc[0]
    assert fila["fecha_afectada"] == pd.Timestamp(datetime(2026, 9, 1, 9, 0, tzinfo=timezone.utc))


def test_actualizada_antes_que_programada_crea_placeholder():
    df = dataframe_vacio()
    df = aplicar_evento(
        df, _evento_actualizada("I-004", "PROGRAMADA", "AFECTADA", datetime(2026, 9, 1, tzinfo=timezone.utc))
    )

    assert len(df) == 1
    fila = df.iloc[0]
    assert fila["intervencionId"] == "I-004"
    assert fila["estado_actual"] == "AFECTADA"
    assert pd.isna(fila["fecha_programada"])


def test_correlationid_repetido_en_varias_intervenciones_no_las_fusiona():
    # Visto en produccion: hasta 5 intervenciones (distinto intervencionId)
    # comparten el mismo correlationId (todas apuntan al mismo caso
    # externo). No tienen que terminar en la misma fila.
    df = dataframe_vacio()
    df = aplicar_evento(
        df, _evento_programada("I-005", datetime(2026, 9, 1, tzinfo=timezone.utc), correlation_id="act-999")
    )
    df = aplicar_evento(
        df, _evento_programada("I-006", datetime(2026, 9, 1, 0, 5, tzinfo=timezone.utc), correlation_id="act-999")
    )

    assert len(df) == 2
    assert set(df["intervencionId"]) == {"I-005", "I-006"}
    assert set(df["correlationId"]) == {"act-999"}


def test_intervencion_organica_queda_con_causa_nula():
    df = dataframe_vacio()
    df = aplicar_evento(df, _evento_programada("I-007", datetime(2026, 9, 1, tzinfo=timezone.utc)))

    fila = df[df["intervencionId"] == "I-007"].iloc[0]
    assert pd.isna(fila["causaId"])
    assert pd.isna(fila["causaTipo"])


def test_estado_actual_no_depende_de_cual_fecha_es_mas_reciente():
    # Visto en produccion (31 de 87 filas reales): fecha_programada (una
    # fecha AGENDADA, no "cuando paso") puede ser posterior a
    # fecha_afectada, si la afectacion se registro antes de la visita
    # programada. estado_actual tiene que seguir siendo AFECTADA.
    df = dataframe_vacio()
    df = aplicar_evento(
        df, _evento_actualizada("I-009", "PROGRAMADA", "AFECTADA", datetime(2026, 11, 10, tzinfo=timezone.utc))
    )
    # La Programada llega despues, con una fecha agendada MAS TARDE que la
    # fecha en que ya se registro la afectacion.
    df = aplicar_evento(df, _evento_programada("I-009", datetime(2026, 11, 15, tzinfo=timezone.utc)))

    fila = df[df["intervencionId"] == "I-009"].iloc[0]
    assert fila["fecha_programada"] > fila["fecha_afectada"]
    assert fila["estado_actual"] == "AFECTADA"
    assert fila["estado_anterior"] == "PROGRAMADA"


def test_intervencion_con_causa_carga_causaid_y_causatipo():
    df = dataframe_vacio()
    df = aplicar_evento(
        df,
        _evento_programada(
            "I-008", datetime(2026, 9, 1, tzinfo=timezone.utc),
            causa_id="EVT-001", causa_tipo="EVENTO_PUBLICO",
        ),
    )

    fila = df[df["intervencionId"] == "I-008"].iloc[0]
    assert fila["causaId"] == "EVT-001"
    assert fila["causaTipo"] == "EVENTO_PUBLICO"
