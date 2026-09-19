"""Capa gold de reclamos.

Timer trigger diario que agrega la tabla completa de silver/Reclamos/
reclamos.parquet por barrio/categoria/prioridad/origenClasificacion/
estado_actual, y guarda el resultado en gold/Reclamos/reclamos_resumen.parquet.

A diferencia de silver, aca no hay incremental/checkpoint: como es una
agregacion (GROUP BY), cada corrida la recalcula entera a partir de la tabla
de silver actual. Los domingos, ademas de sobreescribir la tabla principal,
se guarda una copia con la semana ISO en el nombre (snapshot historico).
"""
import azure.functions as func
import logging
import os
from datetime import datetime, timezone
from io import BytesIO

import pandas as pd
from azure.core.exceptions import ResourceExistsError
from azure.storage.blob import BlobServiceClient

from bp_reclamos_silver import COLUMNA_FECHA_POR_ESTADO, leer_tabla
from bp_reclamos_silver import get_blob_client as get_silver_blob_client

bp = func.Blueprint()

GOLD_CONTAINER_NAME = "gold"
TABLA_RECLAMOS_GOLD_BLOB = "Reclamos/reclamos_resumen.parquet"

COLUMNAS_GRANULARIDAD = ["barrio", "categoria", "prioridad", "origenClasificacion", "estado_actual"]


# --- Acceso a storage (gold) -----------------------------------------------

def _get_gold_container_client():
    connection_string = os.environ["STORAGE_CONNECTION_STRING"]
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(GOLD_CONTAINER_NAME)
    try:
        container_client.create_container()
    except ResourceExistsError:
        pass
    return container_client


def _get_gold_blob_client(nombre_blob: str = TABLA_RECLAMOS_GOLD_BLOB):
    return _get_gold_container_client().get_blob_client(nombre_blob)


# --- Agregacion --------------------------------------------------------------

def _fecha_estado_actual(fila: pd.Series):
    """Fecha en la que el reclamo llego a su estado_actual (busca la
    columna fecha_<estado> correspondiente, vía el mismo mapeo que usa
    bp_reclamos_silver para no duplicar la relacion estado -> columna)."""
    columna = COLUMNA_FECHA_POR_ESTADO.get(fila["estado_actual"])
    if columna is None:
        return pd.NaT
    return fila[columna]


def calcular_resumen(df_silver: pd.DataFrame, fecha_snapshot: datetime | None = None) -> pd.DataFrame:
    """Una fila por combinacion de barrio/categoria/prioridad/
    origenClasificacion/estado_actual, con la cantidad de reclamos y el
    promedio de horas entre fecha_creado y la fecha en la que llegaron a su
    estado_actual. fecha_snapshot marca el momento en que se calculo este
    resumen (por defecto, ahora); se puede pasar explicito para reprocesos."""
    df = df_silver.copy()
    df["_fecha_estado_actual"] = df.apply(_fecha_estado_actual, axis=1)
    df["_horas_hasta_estado_actual"] = (
        (df["_fecha_estado_actual"] - df["fecha_creado"]).dt.total_seconds() / 3600
    )

    resumen = (
        df.groupby(COLUMNAS_GRANULARIDAD, dropna=False)
        .agg(
            row_count=("reclamoId", "count"),
            tiempo_prom_hasta_estado_actual=("_horas_hasta_estado_actual", "mean"),
        )
        .reset_index()
    )
    resumen["fecha_snapshot"] = (fecha_snapshot or datetime.now(timezone.utc)).date()
    return resumen


# --- Nombrado y escritura del resultado -------------------------------------

def nombre_snapshot_semanal(fecha: datetime) -> str:
    """Nombre del parquet de snapshot semanal (se guarda los domingos), con
    la semana ISO del año en la que cae `fecha`."""
    anio, semana, _ = fecha.isocalendar()
    return f"Reclamos/reclamos_resumen_{semana:02d}_{anio}.parquet"


def escribir_resumen(df_resumen: pd.DataFrame, nombre_blob: str = TABLA_RECLAMOS_GOLD_BLOB) -> None:
    buffer = BytesIO()
    df_resumen.to_parquet(buffer, index=False)
    buffer.seek(0)
    _get_gold_blob_client(nombre_blob).upload_blob(buffer, overwrite=True)


# --- Trigger: corre 1 vez por dia, despues de silver ------------------------

@bp.timer_trigger(
    schedule="0 0 1 * * *",
    arg_name="timer",
    run_on_startup=False,
    use_monitor=False,
)
def reclamos_silver_a_gold(timer: func.TimerRequest):
    """Corre una vez por dia a la 1am, una hora despues de que
    bp_reclamos_silver corre a las 00:00, y recalcula el resumen agregado de
    gold desde cero a partir de la tabla de silver completa."""
    logging.info("Corrida diaria de reclamos silver->gold.")

    fecha_snapshot = datetime.now(timezone.utc)

    df_silver = leer_tabla(get_silver_blob_client())
    resumen = calcular_resumen(df_silver, fecha_snapshot=fecha_snapshot)
    escribir_resumen(resumen)

    if fecha_snapshot.weekday() == 6:  # domingo
        nombre_blob = nombre_snapshot_semanal(fecha_snapshot)
        escribir_resumen(resumen, nombre_blob=nombre_blob)
        logging.info(f"Es domingo: snapshot semanal guardado en {nombre_blob}")

    logging.info(f"Resumen gold de reclamos actualizado: {len(resumen)} filas.")
