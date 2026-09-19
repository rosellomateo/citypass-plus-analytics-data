"""Descarga e imprime una tabla Parquet de la capa silver, para verificar a mano
el resultado de una funcion timer-triggered sin tener que escribir el snippet
de pandas cada vez.

Uso desde terminal:
    python scripts/ver_tabla_silver.py                              # reclamos (default)
    python scripts/ver_tabla_silver.py movilidad
    python scripts/ver_tabla_silver.py reclamos --id test-reclamo-001
    python scripts/ver_tabla_silver.py movilidad --id viaje-test-001 --rows 5
    python scripts/ver_tabla_silver.py --container silver --path "OtraTabla/otra.parquet"

Uso desde el editor (boton "Run", sin terminal):
    Editar los valores del llamado a mostrar_tabla() en el bloque
    `if __name__ == "__main__":` al final del archivo.
"""
import argparse
import sys
from io import BytesIO

import pandas as pd

from _storage import blob_service_client

CONTAINER = "silver"

# Agregar una entrada por cada dominio nuevo que tenga su propia tabla de silver.
TABLAS = {
    "reclamos": {"path": "Reclamos/reclamos.parquet", "columna_id": "reclamoId"},
    "movilidad": {"path": "Movilidad Urbana/viajes.parquet", "columna_id": "viajeId"},
}


def mostrar_tabla(dominio="reclamos", container=None, path=None, id=None, rows=None):
    """Logica principal: descarga la tabla y la imprime. Llamable directo
    desde Python (sin pasar por argparse/terminal) con parametros normales."""
    tabla = TABLAS[dominio]
    container = container or CONTAINER
    path = path or tabla["path"]
    columna_id = tabla["columna_id"]

    blob_client = blob_service_client().get_container_client(container).get_blob_client(path)

    contenido = blob_client.download_blob().readall()
    df = pd.read_parquet(BytesIO(contenido))

    if id and columna_id in df.columns:
        df = df[df[columna_id] == id]

    if rows:
        df = df.head(rows)

    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 200)

    print(f"{container}/{path} -> {len(df)} filas\n")
    print(df.T if len(df) <= 3 else df)


def main():
    """Wrapper de linea de comandos: parsea sys.argv y llama a mostrar_tabla()."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "dominio",
        nargs="?",
        default="reclamos",
        choices=TABLAS.keys(),
        help="Que tabla de silver mostrar (default: reclamos)",
    )
    parser.add_argument("--container", default=None, help="Override del container (default: silver)")
    parser.add_argument("--path", default=None, help="Override del path del blob (ignora --dominio)")
    parser.add_argument("--id", default=None, help="Filtra por la columna id de esa tabla (reclamoId/viajeId/etc)")
    parser.add_argument("--rows", type=int, default=None, help="Limita la cantidad de filas mostradas")
    args = parser.parse_args()

    mostrar_tabla(dominio=args.dominio, container=args.container, path=args.path, id=args.id, rows=args.rows)


if __name__ == "__main__":
    if len(sys.argv) > 1:
        # Se corrio desde terminal con argumentos (ej. "python ver_tabla_silver.py movilidad").
        main()
    else:
        # Se corrio desde el editor sin argumentos: editar estos valores a mano.
        mostrar_tabla(dominio="movilidad")
