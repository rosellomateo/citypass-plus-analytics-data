"""Silver de Residuos: la particularidad es que una RecoleccionCompletada
puede no tener alertaId (viene de un recorrido programado, no de una
alerta) -- en ese caso NO debe reflejarse en esta tabla."""
from datetime import datetime, timezone

from residuos.bp_residuos_silver import aplicar_evento, dataframe_vacio


def _evento_alerta(alerta_id: str, occurred_at: datetime) -> dict:
    return {
        "metadata": {"eventType": "com.citypass.residuos.AlertaGenerada"},
        "data": {
            "alertaId": alerta_id,
            "contenedorId": "cont-001",
            "tipoAlerta": "LLENO",
            "nivelLlenado": 85,
            "zona": "Caballito",
            "prioridad": "MEDIA",
            "correlationId": f"corr-{alerta_id}",
            "occurredAt": occurred_at.isoformat(),
        },
    }


def _evento_recoleccion(recoleccion_id: str, occurred_at: datetime, alerta_id: str | None) -> dict:
    return {
        "metadata": {"eventType": "com.citypass.residuos.RecoleccionCompletada"},
        "data": {
            "recoleccionId": recoleccion_id,
            "contenedorId": "cont-001",
            "alertaId": alerta_id,
            "camionId": "camion-01",
            "correlationId": f"corr-{recoleccion_id}",
            "occurredAt": occurred_at.isoformat(),
        },
    }


def test_alerta_generada_nueva_queda_sin_resolver():
    df = dataframe_vacio()
    df = aplicar_evento(df, _evento_alerta("A-001", datetime(2026, 9, 1, tzinfo=timezone.utc)))

    fila = df.iloc[0]
    assert fila["alertaId"] == "A-001"
    assert fila["resuelta"] == False  # noqa: E712


def test_recoleccion_sin_alertaid_no_se_refleja_en_la_tabla():
    df = dataframe_vacio()

    resultado = aplicar_evento(
        df, _evento_recoleccion("REC-001", datetime(2026, 9, 1, tzinfo=timezone.utc), alerta_id=None)
    )

    # Es un recorrido programado, no una respuesta a una alerta -- no hay
    # nada que actualizar en esta tabla.
    assert resultado is None
    assert len(df) == 0


def test_alerta_resuelta_por_su_recoleccion_calcula_el_tiempo():
    df = dataframe_vacio()
    alerta_en = datetime(2026, 9, 1, 8, 0, tzinfo=timezone.utc)
    recoleccion_en = datetime(2026, 9, 1, 8, 45, tzinfo=timezone.utc)

    df = aplicar_evento(df, _evento_alerta("A-002", alerta_en))
    df = aplicar_evento(df, _evento_recoleccion("REC-002", recoleccion_en, alerta_id="A-002"))

    fila = df[df["alertaId"] == "A-002"].iloc[0]
    assert fila["resuelta"] == True  # noqa: E712
    assert fila["tiempo_resolucion_minutos"] == 45.0
