"""Gold de Movilidad: los viajes 'en curso' (sin duracion) se excluyen del
resumen, y la duracion se agrupa en franjas (<15min, 15-30min, 30-60min,
>1hs)."""
from datetime import datetime, timezone

import pandas as pd

from movilidad.bp_movilidad_gold import calcular_resumen


def test_resumen_excluye_viajes_en_curso_y_bucketiza_la_duracion():
    inicio = datetime(2026, 9, 1, 8, 0, tzinfo=timezone.utc)
    filas = [
        {  # viaje completo, 20 minutos -> bucket "15-30min"
            "viajeId": "V-001", "estacionInicio": "liniers-02",
            "horaInicio": inicio, "duracion_minutos": 20.0,
        },
        {  # viaje en curso, sin duracion -> se tiene que excluir del resumen
            "viajeId": "V-002", "estacionInicio": "liniers-02",
            "horaInicio": inicio, "duracion_minutos": None,
        },
    ]
    df_silver = pd.DataFrame(filas)
    df_silver["horaInicio"] = pd.to_datetime(df_silver["horaInicio"], utc=True)

    resumen = calcular_resumen(df_silver, fecha_snapshot=datetime(2026, 9, 5, tzinfo=timezone.utc))

    assert len(resumen) == 1
    fila = resumen.iloc[0]
    assert fila["cantidadViajes"] == 1
    assert fila["duracionViaje"] == "15-30min"
