"""Silver de Autenticacion: a diferencia del resto de los dominios, aca cada
evento es una fila nueva (no hay entidad con ciclo de vida que reconstruir),
y la deduplicacion es por auth_event_id en vez de por columna de fecha.
"""
from datetime import datetime, timezone

import pandas as pd

from autenticacion.bp_autenticacion_silver import aplicar_evento, dataframe_vacio


def _evento(tipo: str, auth_event_id: str, occurred_at_ms: int, **data_extra) -> dict:
    data = {
        "authEventId": auth_event_id,
        "actorSub": "U-001",
        "occurredAt": occurred_at_ms,
        **data_extra,
    }
    return {
        "metadata": {"eventType": f"com.citypass.auth.{tipo}"},
        "data": data,
    }


def test_login_exitoso_crea_una_fila_con_todos_los_campos():
    df = dataframe_vacio()
    df = aplicar_evento(
        df,
        _evento(
            "LoginExitoso", "AE-001", 1793362500000,
            username="nicolas", department="reclamos", clientId="citypass-reclamos-web",
            chainId=None, ipAddress="192.168.1.10", userAgent="Mozilla/5.0",
        ),
        ingested_at=datetime(2026, 9, 1, 9, 0, tzinfo=timezone.utc),
    )

    assert len(df) == 1
    fila = df.iloc[0]
    assert fila["event_type"] == "com.citypass.auth.LoginExitoso"
    assert fila["auth_event_id"] == "AE-001"
    assert fila["actor_sub"] == "U-001"
    assert fila["username"] == "nicolas"
    assert fila["department"] == "reclamos"
    assert fila["client_id"] == "citypass-reclamos-web"
    assert pd.isna(fila["chain_id"])
    assert fila["ip_address"] == "192.168.1.10"
    assert fila["user_agent"] == "Mozilla/5.0"
    assert fila["event_time_utc"] == pd.Timestamp(1793362500000, unit="ms", tz="UTC")
    assert fila["ingested_at"] == pd.Timestamp(datetime(2026, 9, 1, 9, 0, tzinfo=timezone.utc))


def test_los_3_tipos_de_evento_se_guardan_en_la_misma_tabla():
    df = dataframe_vacio()
    df = aplicar_evento(df, _evento("LoginExitoso", "AE-001", 1793362500000), ingested_at=datetime(2026, 9, 1, tzinfo=timezone.utc))
    df = aplicar_evento(df, _evento("TokenRenovado", "AE-002", 1793366100000, chainId="CH-1"), ingested_at=datetime(2026, 9, 1, tzinfo=timezone.utc))
    df = aplicar_evento(df, _evento("SesionCerrada", "AE-003", 1793369700000), ingested_at=datetime(2026, 9, 1, tzinfo=timezone.utc))

    assert len(df) == 3
    assert set(df["event_type"]) == {
        "com.citypass.auth.LoginExitoso",
        "com.citypass.auth.TokenRenovado",
        "com.citypass.auth.SesionCerrada",
    }


def test_autheventid_duplicado_se_ignora():
    df = dataframe_vacio()
    df = aplicar_evento(df, _evento("LoginExitoso", "AE-001", 1793362500000), ingested_at=datetime(2026, 9, 1, tzinfo=timezone.utc))
    # Mismo authEventId, reintento de entrega -- no debe duplicarse.
    df = aplicar_evento(df, _evento("LoginExitoso", "AE-001", 1793362500000), ingested_at=datetime(2026, 9, 1, 0, 5, tzinfo=timezone.utc))

    assert len(df) == 1


def test_evento_de_tipo_inesperado_se_ignora():
    df = dataframe_vacio()
    evento = {"metadata": {"eventType": "com.citypass.auth.OtraCosa"}, "data": {"authEventId": "AE-999"}}

    resultado = aplicar_evento(df, evento, ingested_at=datetime(2026, 9, 1, tzinfo=timezone.utc))

    assert resultado is None


def test_occurredat_invalido_queda_con_fecha_nula_sin_romper():
    df = dataframe_vacio()
    evento = {
        "metadata": {"eventType": "com.citypass.auth.LoginExitoso"},
        "data": {"authEventId": "AE-100", "actorSub": "U-001", "occurredAt": "no-es-un-epoch"},
    }

    df = aplicar_evento(df, evento, ingested_at=datetime(2026, 9, 1, tzinfo=timezone.utc))

    assert pd.isna(df.iloc[0]["event_time_utc"])
