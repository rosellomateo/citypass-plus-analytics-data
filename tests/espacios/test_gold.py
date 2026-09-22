"""Gold de Espacios: los inscriptos y el % de ocupacion solo deben contar
reservas CONFIRMADA -- una reserva CANCELADA no debe sumar personas."""
from datetime import datetime, timezone

import pandas as pd

from espacios.bp_espacios_gold import calcular_resumen


def test_resumen_solo_cuenta_inscriptos_de_reservas_confirmadas():
    filas = [
        {
            "reservaId": "RES-001", "recursoId": "evento-cultural-01", "tipoReserva": "EVENTO",
            "categoria": "CULTURAL", "zona": "Palermo", "cupoMaximo": 10,
            "cantidadPersonas": 4, "confirmada": True, "cancelada": False,
        },
        {
            "reservaId": "RES-002", "recursoId": "evento-cultural-01", "tipoReserva": "EVENTO",
            "categoria": "CULTURAL", "zona": "Palermo", "cupoMaximo": 10,
            "cantidadPersonas": 5, "confirmada": False, "cancelada": True,
        },
    ]
    df_silver = pd.DataFrame(filas)

    resumen = calcular_resumen(df_silver, fecha_snapshot=datetime(2026, 9, 5, tzinfo=timezone.utc))

    assert len(resumen) == 1
    fila = resumen.iloc[0]
    assert fila["cantidadConfirmadas"] == 1
    assert fila["cantidadCanceladas"] == 1
    # Los 5 de la reserva CANCELADA no deben sumar como inscriptos.
    assert fila["inscriptos"] == 4
    assert fila["pctOcupacion"] == 40.0
