"""Gold de Autenticacion: el resumen cuenta eventos por
department/username/event_type, a partir de una tabla de Silver armada a
mano. No lee ni escribe ningun archivo, no se conecta a Azure.
"""
from datetime import datetime, timezone

import pandas as pd

from autenticacion.bp_autenticacion_gold import calcular_resumen


def _fila(auth_event_id, department, username, event_type):
    return {
        "auth_event_id": auth_event_id,
        "department": department,
        "username": username,
        "event_type": event_type,
        "actor_sub": "U-001",
        "event_time_utc": datetime(2026, 9, 1, tzinfo=timezone.utc),
        "ingested_at": datetime(2026, 9, 1, tzinfo=timezone.utc),
    }


def test_calcular_resumen_cuenta_por_department_username_event_type():
    filas = [
        _fila("AE-001", "reclamos", "nicolas", "com.citypass.auth.LoginExitoso"),
        _fila("AE-002", "reclamos", "nicolas", "com.citypass.auth.LoginExitoso"),
        _fila("AE-003", "reclamos", "nicolas", "com.citypass.auth.TokenRenovado"),
        _fila("AE-004", "emergencias", "juan", "com.citypass.auth.LoginExitoso"),
    ]
    df_silver = pd.DataFrame(filas)

    resumen = calcular_resumen(df_silver, fecha_snapshot=datetime(2026, 9, 5, tzinfo=timezone.utc))

    assert len(resumen) == 3

    fila_login_nicolas = resumen[
        (resumen["department"] == "reclamos")
        & (resumen["username"] == "nicolas")
        & (resumen["event_type"] == "com.citypass.auth.LoginExitoso")
    ].iloc[0]
    assert fila_login_nicolas["cantidadEventos"] == 2

    fila_refresh_nicolas = resumen[resumen["event_type"] == "com.citypass.auth.TokenRenovado"].iloc[0]
    assert fila_refresh_nicolas["cantidadEventos"] == 1


def test_calcular_resumen_no_pierde_filas_con_username_nulo():
    # SesionCerrada por revocacion masiva viene con username=None.
    filas = [
        _fila("AE-005", "reclamos", None, "com.citypass.auth.SesionCerrada"),
        _fila("AE-006", "reclamos", None, "com.citypass.auth.SesionCerrada"),
    ]
    df_silver = pd.DataFrame(filas)

    resumen = calcular_resumen(df_silver, fecha_snapshot=datetime(2026, 9, 5, tzinfo=timezone.utc))

    assert len(resumen) == 1
    assert resumen.iloc[0]["cantidadEventos"] == 2
    assert pd.isna(resumen.iloc[0]["username"])
