"""Descarga e imprime una tabla Parquet de la capa silver, para verificar a mano
el resultado de una funcion blob-triggered sin tener que escribir el snippet
de pandas cada vez.

Uso:
    python scripts/ver_tabla_silver.py
    python scripts/ver_tabla_silver.py --path Reclamos/reclamos.parquet --reclamo-id test-reclamo-001
    python scripts/ver_tabla_silver.py --container silver --path OtraTabla/otra.parquet
"""
import argparse
from io import BytesIO

import pandas as pd

from _storage import blob_service_client


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--container", default="silver")
    parser.add_argument("--path", default="Reclamos/reclamos.parquet")
    parser.add_argument("--reclamo-id", default=None, help="Filtra por columna reclamoId")
    parser.add_argument("--rows", type=int, default=None, help="Limita la cantidad de filas mostradas")
    args = parser.parse_args()

    blob_client = blob_service_client().get_container_client(args.container).get_blob_client(args.path)

    contenido = blob_client.download_blob().readall()
    df = pd.read_parquet(BytesIO(contenido))

    if args.reclamo_id and "reclamoId" in df.columns:
        df = df[df["reclamoId"] == args.reclamo_id]

    if args.rows:
        df = df.head(args.rows)

    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 200)

    print(f"{args.container}/{args.path} -> {len(df)} filas\n")
    print(df.T if len(df) <= 3 else df)


if __name__ == "__main__":
    main()
