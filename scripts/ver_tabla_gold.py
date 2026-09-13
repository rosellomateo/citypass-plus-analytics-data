"""Descarga e imprime el resumen agregado de gold/Reclamos/reclamos_resumen.parquet.

Uso:
    python scripts/ver_tabla_gold.py
    python scripts/ver_tabla_gold.py --barrio Caballito
    python scripts/ver_tabla_gold.py --categoria BACHES --prioridad ALTA
"""
import argparse
from io import BytesIO

import pandas as pd

from _storage import blob_service_client

CONTAINER = "gold"
PATH = "Reclamos/reclamos_resumen.parquet"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--barrio", default=None)
    parser.add_argument("--categoria", default=None)
    parser.add_argument("--prioridad", default=None)
    parser.add_argument("--origen-clasificacion", default=None, help="Filtra por origenClasificacion")
    parser.add_argument("--estado-actual", default=None)
    args = parser.parse_args()

    blob_client = blob_service_client().get_container_client(CONTAINER).get_blob_client(PATH)
    contenido = blob_client.download_blob().readall()
    df = pd.read_parquet(BytesIO(contenido))

    filtros = {
        "barrio": args.barrio,
        "categoria": args.categoria,
        "prioridad": args.prioridad,
        "origenClasificacion": args.origen_clasificacion,
        "estado_actual": args.estado_actual,
    }
    for columna, valor in filtros.items():
        if valor is not None:
            df = df[df[columna] == valor]

    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 200)

    print(f"{CONTAINER}/{PATH} -> {len(df)} filas\n")
    print(df.sort_values("row_count", ascending=False).to_string(index=False))


if __name__ == "__main__":
    main()
