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


# --- Casos 1 y 4: causa de la afectacion, CANCELADA, COMPLETADA, .v2 -------

def _actualizada_con_causa(intervencion_id, estado_nuevo, updated_at, causa_tipo,
                           event_type="com.citypass.residuos.IntervencionActualizada", barrio=None):
    evento = _evento_actualizada(intervencion_id, "PROGRAMADA", estado_nuevo, updated_at)
    evento["metadata"]["eventType"] = event_type
    evento["data"]["causaId"] = f"causa-{causa_tipo}"
    evento["data"]["causaTipo"] = causa_tipo
    if barrio is not None:
        evento["data"]["barrio"] = barrio
    return evento


def test_la_causa_de_la_afectacion_no_pisa_la_de_la_programacion():
    t0 = datetime(2026, 9, 1, 8, 0, tzinfo=timezone.utc)
    df = dataframe_vacio()
    df = aplicar_evento(df, _evento_programada("I-1", t0, causa_id="R-9", causa_tipo="RECLAMO"))
    df = aplicar_evento(df, _actualizada_con_causa("I-1", "AFECTADA", t0, "EVENTO_CLIMATICO"))

    fila = df.iloc[0]
    assert fila["causaTipo"] == "RECLAMO"
    assert fila["causaId"] == "R-9"
    assert fila["causaTipoAfectacion"] == "EVENTO_CLIMATICO"
    assert fila["estado_actual"] == "AFECTADA"


def test_las_dos_causas_sobreviven_aunque_la_actualizada_llegue_primero():
    t0 = datetime(2026, 9, 1, 8, 0, tzinfo=timezone.utc)
    df = dataframe_vacio()
    df = aplicar_evento(df, _actualizada_con_causa("I-1", "AFECTADA", t0, "INCIDENTE_VIAL"))
    df = aplicar_evento(df, _evento_programada("I-1", t0, causa_tipo="EVENTO_PUBLICO", causa_id="E-1"))

    fila = df.iloc[0]
    assert fila["causaTipo"] == "EVENTO_PUBLICO"
    assert fila["causaTipoAfectacion"] == "INCIDENTE_VIAL"
    assert fila["camionId"] == "QA124ZZ"  # la Programada (camionId null) no lo pisa
    assert fila["estado_actual"] == "AFECTADA"


def test_cancelada_se_aplica_y_es_el_estado_actual():
    t0 = datetime(2026, 9, 1, 8, 0, tzinfo=timezone.utc)
    df = dataframe_vacio()
    df = aplicar_evento(df, _evento_programada("I-1", t0))
    df = aplicar_evento(df, _actualizada_con_causa("I-1", "CANCELADA", t0, "INCIDENTE_VIAL"))

    fila = df.iloc[0]
    assert fila["estado_actual"] == "CANCELADA"
    assert bool(fila["cancelada"]) is True
    assert bool(fila["afectada"]) is False


def test_topico_v2_se_procesa_igual_y_trae_barrio_si_no_hay_coordenadas():
    t0 = datetime(2026, 9, 1, 8, 0, tzinfo=timezone.utc)
    df = dataframe_vacio()
    df = aplicar_evento(df, _actualizada_con_causa(
        "I-1", "AFECTADA", t0, "EVENTO_CLIMATICO",
        event_type="com.citypass.residuos.IntervencionActualizada.v2", barrio="Palermo",
    ))

    fila = df.iloc[0]
    assert fila["estado_actual"] == "AFECTADA"
    assert fila["barrio"] == "Palermo"


def test_completada_cierra_la_intervencion():
    t0 = datetime(2026, 9, 1, 8, 0, tzinfo=timezone.utc)
    df = dataframe_vacio()
    df = aplicar_evento(df, _evento_programada("I-1", t0))
    df = aplicar_evento(df, {
        "metadata": {"eventType": "com.citypass.residuos.IntervencionCompletada"},
        "data": {"intervencionId": "I-1", "emergenciaId": "E-7",
                 "completadaEn": _epoch_ms(datetime(2026, 9, 1, 10, 0, tzinfo=timezone.utc))},
    })

    fila = df.iloc[0]
    assert fila["estado_actual"] == "COMPLETADA"
    assert bool(fila["completada"]) is True
    assert fila["emergenciaId"] == "E-7"


def test_el_cierre_mas_reciente_define_el_estado_actual():
    t0 = datetime(2026, 9, 1, 8, 0, tzinfo=timezone.utc)
    df = dataframe_vacio()
    df = aplicar_evento(df, _evento_programada("I-1", t0))
    df = aplicar_evento(df, _actualizada_con_causa("I-1", "AFECTADA", datetime(2026, 9, 1, 9, 0, tzinfo=timezone.utc), "EVENTO_CLIMATICO"))
    df = aplicar_evento(df, {
        "metadata": {"eventType": "com.citypass.residuos.IntervencionCompletada"},
        "data": {"intervencionId": "I-1", "completadaEn": "2026-09-01T12:00:00Z"},
    })

    assert df.iloc[0]["estado_actual"] == "COMPLETADA"


def test_barrio_sale_de_las_coordenadas():
    t0 = datetime(2026, 9, 1, 8, 0, tzinfo=timezone.utc)
    evento = _evento_programada("I-1", t0)
    evento["data"]["latitud"] = -34.5875   # Palermo
    evento["data"]["longitud"] = -58.4205
    df = aplicar_evento(dataframe_vacio(), evento)

    assert df.iloc[0]["barrio"] == "PALERMO" or df.iloc[0]["barrio"].upper() == "PALERMO"


def test_tabla_vieja_sin_columnas_nuevas_se_completa():
    from residuos.bp_residuos_silver import completar_columnas
    vieja = dataframe_vacio().drop(columns=["fecha_cancelada", "fecha_completada", "causaTipoAfectacion",
                                            "causaIdAfectacion", "barrio", "cancelada", "completada"])
    t0 = datetime(2026, 9, 1, 8, 0, tzinfo=timezone.utc)
    df = aplicar_evento(vieja, _actualizada_con_causa("I-1", "CANCELADA", t0, "INCIDENTE_VIAL"))

    assert list(completar_columnas(df).columns) == list(dataframe_vacio().columns)
    assert df.iloc[0]["estado_actual"] == "CANCELADA"
