"""Reconstruye silver/Gestion de Residuos Inteligente/alertas.parquet desde
cero, reprocesando TODOS los eventos que ya existen en bronze/Gestion de
Residuos Inteligente/.

Ver scripts/reprocesar_reclamos_silver.py para la explicacion completa del
por que hace falta esto (receipts del blob trigger).

Uso:
    python scripts/reprocesar_residuos_silver.py
"""
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from _storage import blob_service_client, connection_string  # noqa: E402

os.environ.setdefault("STORAGE_CONNECTION_STRING", connection_string())

from bp_residuos_silver import (  # noqa: E402
    aplicar_evento,
    dataframe_vacio,
    escribir_tabla,
    get_blob_client,
)

BRONZE_CONTAINER = "bronze"
PREFIJO_RESIDUOS = "Gestion de Residuos Inteligente/"


def main():
    container_client = blob_service_client().get_container_client(BRONZE_CONTAINER)

    df = dataframe_vacio()
    procesados = 0
    ignorados = 0

    for blob in container_client.list_blobs(name_starts_with=PREFIJO_RESIDUOS):
        contenido = container_client.download_blob(blob.name).readall()

        try:
            evento = json.loads(contenido)
        except json.JSONDecodeError:
            print(f"Ignorado (no es JSON valido, {len(contenido)} bytes): {blob.name}")
            ignorados += 1
            continue

        df_actualizado = aplicar_evento(df, evento)
        if df_actualizado is None:
            print(f"Ignorado (eventType inesperado o RecoleccionCompletada sin alertaId): {blob.name}")
            ignorados += 1
            continue

        df = df_actualizado
        procesados += 1

    escribir_tabla(get_blob_client(), df)
    print(f"Listo: {procesados} eventos procesados, {ignorados} ignorados, {len(df)} filas en la tabla.")


if __name__ == "__main__":
    main()
