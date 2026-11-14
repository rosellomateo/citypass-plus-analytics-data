"""Reconstruye silver/Movilidad Urbana/viajes.parquet y
silver/Movilidad Urbana/estaciones_afectadas.parquet desde cero,
reprocesando TODOS los eventos que ya existen en bronze/Movilidad Urbana/
(un solo recorrido de bronze para las dos tablas).

Ver scripts/reprocesar_reclamos_silver.py para la explicacion completa del
por que hace falta esto (receipts del blob trigger).

Uso:
    python scripts/reprocesar_movilidad_silver.py
"""
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from _storage import blob_service_client, connection_string  # noqa: E402

os.environ.setdefault("STORAGE_CONNECTION_STRING", connection_string())

from movilidad.bp_movilidad_silver import (  # noqa: E402
    aplicar_evento,
    aplicar_evento_estacion,
    dataframe_vacio,
    dataframe_vacio_estaciones,
    escribir_tabla,
    get_blob_client,
    get_blob_client_estaciones,
)

BRONZE_CONTAINER = "bronze"
PREFIJO_MOVILIDAD = "Movilidad Urbana/"


def main():
    container_client = blob_service_client().get_container_client(BRONZE_CONTAINER)

    df_viajes = dataframe_vacio()
    df_estaciones = dataframe_vacio_estaciones()
    procesados_viajes = 0
    procesados_estaciones = 0
    ignorados = 0

    for blob in container_client.list_blobs(name_starts_with=PREFIJO_MOVILIDAD):
        contenido = container_client.download_blob(blob.name).readall()

        try:
            evento = json.loads(contenido)
        except json.JSONDecodeError:
            print(f"Ignorado (no es JSON valido, {len(contenido)} bytes): {blob.name}")
            ignorados += 1
            continue

        df_viajes_actualizado = aplicar_evento(df_viajes, evento)
        if df_viajes_actualizado is not None:
            df_viajes = df_viajes_actualizado
            procesados_viajes += 1
            continue

        df_estaciones_actualizado = aplicar_evento_estacion(df_estaciones, evento)
        if df_estaciones_actualizado is not None:
            df_estaciones = df_estaciones_actualizado
            procesados_estaciones += 1
            continue

        print(f"Ignorado (eventType inesperado): {blob.name}")
        ignorados += 1

    escribir_tabla(get_blob_client(), df_viajes)
    escribir_tabla(get_blob_client_estaciones(), df_estaciones)
    print(
        f"Listo: {procesados_viajes} viajes procesados ({len(df_viajes)} filas), "
        f"{procesados_estaciones} eventos de estacion procesados ({len(df_estaciones)} filas), "
        f"{ignorados} ignorados."
    )


if __name__ == "__main__":
    main()
