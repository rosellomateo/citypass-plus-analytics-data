"""Gold de Residuos: el nivel de llenado se agrupa en franjas de 20, y el
tiempo promedio de resolucion tiene que ignorar (no contar como 0) las
alertas que todavia no se resolvieron."""
from datetime import datetime, timezone

import pandas as pd

from residuos.bp_residuos_gold import calcular_resumen


def test_resumen_bucketiza_nivel_y_promedia_solo_las_resueltas():
    filas = [
        {
            "alertaId": "A-001", "zona": "Caballito", "tipoAlerta": "LLENO",
            "prioridad": "MEDIA", "nivelLlenado": 85,
            "resuelta": True, "tiempo_resolucion_minutos": 30.0,
        },
        {
            "alertaId": "A-002", "zona": "Caballito", "tipoAlerta": "LLENO",
            "prioridad": "MEDIA", "nivelLlenado": 90,
            "resuelta": False, "tiempo_resolucion_minutos": None,
        },
    ]
    df_silver = pd.DataFrame(filas)

    resumen = calcular_resumen(df_silver, fecha_snapshot=datetime(2026, 9, 5, tzinfo=timezone.utc))

    assert len(resumen) == 1
    fila = resumen.iloc[0]
    assert fila["rangoNivelLlenado"] == "80-100"
    assert fila["cantidadAlertas"] == 2
    assert fila["cantidadResueltas"] == 1
    # El promedio ignora la alerta sin resolver (NaN), no la cuenta como 0.
    assert fila["tiempoPromResolucion"] == 30.0
