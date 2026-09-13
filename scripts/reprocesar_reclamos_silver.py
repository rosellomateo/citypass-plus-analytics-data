"""Reconstruye silver/Reclamos/reclamos.parquet desde cero, reprocesando
TODOS los eventos que ya existen en bronze/Reclamos/.

Hace falta porque el blob trigger de Azure guarda un "recibo" de cada blob
que ya vio: si cambia la logica de bp_reclamos_silver.py, los eventos viejos
no se vuelven a disparar solos. Este script re-juega el bronze entero usando
las mismas funciones que usa el trigger (aplicar_evento), asi que el
resultado es identico al que hubiera dado el trigger si hubiera procesado
todo en orden.

Uso:
    python scripts/reprocesar_reclamos_silver.py
"""
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from _storage import blob_service_client, connection_string  # noqa: E402

# bp_reclamos_silver.get_blob_client() lee la connection string de una
# variable de entorno (asi la resuelve Azure Functions en produccion); al
# correr este script suelto hay que cargarla nosotros primero.
os.environ.setdefault("STORAGE_CONNECTION_STRING", connection_string())

from bp_reclamos_silver import (  # noqa: E402
    aplicar_evento,
    dataframe_vacio,
    escribir_tabla,
    get_blob_client,
)

BRONZE_CONTAINER = "bronze"
PREFIJO_RECLAMOS = "Reclamos/"


def main():
    container_client = blob_service_client().get_container_client(BRONZE_CONTAINER)

    df = dataframe_vacio()
    procesados = 0
    ignorados = 0

    for blob in container_client.list_blobs(name_starts_with=PREFIJO_RECLAMOS):
        contenido = container_client.download_blob(blob.name).readall()

        try:
            evento = json.loads(contenido)
        except json.JSONDecodeError:
            print(f"Ignorado (no es JSON valido, {len(contenido)} bytes): {blob.name}")
            ignorados += 1
            continue

        df_actualizado = aplicar_evento(df, evento)
        if df_actualizado is None:
            print(f"Ignorado (eventType inesperado): {blob.name}")
            ignorados += 1
            continue

        df = df_actualizado
        procesados += 1

    escribir_tabla(get_blob_client(), df)
    print(f"Listo: {procesados} eventos procesados, {ignorados} ignorados, {len(df)} filas en la tabla.")


if __name__ == "__main__":
    main()
