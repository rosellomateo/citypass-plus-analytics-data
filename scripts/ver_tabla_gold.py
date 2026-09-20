"""Descarga e imprime el resumen agregado de una tabla de la capa gold.

Uso desde terminal:
    python scripts/ver_tabla_gold.py                                          # reclamos (default)
    python scripts/ver_tabla_gold.py movilidad
    python scripts/ver_tabla_gold.py reclamos --where barrio=Caballito --where categoria=BACHES
    python scripts/ver_tabla_gold.py movilidad --where estacionInicio=est-retiro-02
    python scripts/ver_tabla_gold.py residuos --where zona=Palermo
    python scripts/ver_tabla_gold.py --path "Reclamos/reclamos_resumen_38_2026.parquet"  # snapshot semanal puntual

Uso desde el editor (boton "Run", sin terminal):
    Editar los valores del llamado a mostrar_tabla() en el bloque
    `if __name__ == "__main__":` al final del archivo.
"""
import argparse
import sys
from io import BytesIO

import pandas as pd

from _storage import blob_service_client

CONTAINER = "gold"

# Agregar una entrada por cada dominio nuevo que tenga su propia tabla de gold.
TABLAS = {
    "reclamos": {"path": "Reclamos/reclamos_resumen.parquet", "columna_orden": "row_count"},
    "movilidad": {"path": "Movilidad Urbana/viajes_resumen.parquet", "columna_orden": "cantidadViajes"},
    "residuos": {"path": "Gestion de Residuos Inteligente/alertas_resumen.parquet", "columna_orden": "cantidadAlertas"},
    "espacios": {"path": "Espacios Publicos y Cultura/reservas_resumen.parquet", "columna_orden": "cantidadTotal"},
    "emergencias": {"path": "Emergencias y Seguridad/emergencias_resumen.parquet", "columna_orden": "cantidadEmergencias"},
}


def _parse_where(pares: list) -> dict:
    """Convierte ["barrio=Caballito", "categoria=BACHES"] en {"barrio": "Caballito", ...}."""
    where = {}
    for par in pares:
        columna, _, valor = par.partition("=")
        where[columna] = valor
    return where


def mostrar_tabla(dominio="reclamos", container=None, path=None, where=None, rows=None):
    """Logica principal: descarga el resumen y lo imprime. Llamable directo
    desde Python (sin pasar por argparse/terminal) con parametros normales.
    `where` es un dict {columna: valor} para filtrar (columnas invalidas se
    ignoran con un aviso, ya que cada dominio tiene las suyas)."""
    tabla = TABLAS[dominio]
    container = container or CONTAINER
    path = path or tabla["path"]

    blob_client = blob_service_client().get_container_client(container).get_blob_client(path)
    contenido = blob_client.download_blob().readall()
    df = pd.read_parquet(BytesIO(contenido))

    for columna, valor in (where or {}).items():
        if columna not in df.columns:
            print(f"(aviso: la tabla no tiene columna '{columna}', se ignora ese filtro)")
            continue
        df = df[df[columna].astype(str) == valor]

    columna_orden = tabla["columna_orden"]
    if columna_orden in df.columns:
        df = df.sort_values(columna_orden, ascending=False)

    if rows:
        df = df.head(rows)

    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 200)

    print(f"{container}/{path} -> {len(df)} filas\n")
    print(df.to_string(index=False))


def main():
    """Wrapper de linea de comandos: parsea sys.argv y llama a mostrar_tabla()."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "dominio",
        nargs="?",
        default="reclamos",
        choices=TABLAS.keys(),
        help="Que tabla de gold mostrar (default: reclamos)",
    )
    parser.add_argument("--container", default=None, help="Override del container (default: gold)")
    parser.add_argument(
        "--path", default=None,
        help="Override del path del blob (ignora --dominio; util para leer un snapshot semanal puntual)",
    )
    parser.add_argument(
        "--where", action="append", default=[], metavar="columna=valor",
        help="Filtra por columna=valor, se puede repetir",
    )
    parser.add_argument("--rows", type=int, default=None, help="Limita la cantidad de filas mostradas")
    args = parser.parse_args()

    mostrar_tabla(
        dominio=args.dominio,
        container=args.container,
        path=args.path,
        where=_parse_where(args.where),
        rows=args.rows,
    )


if __name__ == "__main__":
    if len(sys.argv) > 1:
        # Se corrio desde terminal con argumentos (ej. "python ver_tabla_gold.py movilidad").
        main()
    else:
        # Se corrio desde el editor sin argumentos: editar estos valores a mano.
        mostrar_tabla(dominio="reclamos ")
