"""Capa gold de autenticacion (LDAP / Login Federado, Grupo 9).

Timer trigger diario que agrega silver/LDAP/eventos_autenticacion.parquet
por [department, username, event_type], contando cuantos eventos cayeron en
cada combinacion, y guarda el resultado en gold/LDAP/autenticacion_resumen.parquet.

Sin incremental/checkpoint, como el resto de las tablas gold: cada corrida
recalcula el resumen entero a partir de la tabla de silver actual. Los
domingos, ademas de sobreescribir la tabla principal, se guarda una copia
con la semana ISO en el nombre (snapshot historico).
"""
import azure.functions as func
import logging
import os
from datetime import datetime, timezone
from io import BytesIO

import pandas as pd
from azure.core.exceptions import ResourceExistsError
from azure.storage.blob import BlobServiceClient

from .bp_autenticacion_silver import leer_tabla
from .bp_autenticacion_silver import get_blob_client as get_silver_blob_client

bp = func.Blueprint()

GOLD_CONTAINER_NAME = "gold"
TABLA_AUTENTICACION_GOLD_BLOB = "LDAP/autenticacion_resumen.parquet"

COLUMNAS_GRANULARIDAD = ["department", "username", "event_type"]


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


def _get_gold_blob_client(nombre_blob: str = TABLA_AUTENTICACION_GOLD_BLOB):
    return _get_gold_container_client().get_blob_client(nombre_blob)


# --- Agregacion --------------------------------------------------------------

def calcular_resumen(df_silver: pd.DataFrame, fecha_snapshot: datetime | None = None) -> pd.DataFrame:
    """Una fila por combinacion de department/username/event_type, con la
    cantidad de eventos de autenticacion en cada una. fecha_snapshot marca
    el momento en que se calculo este resumen (por defecto, ahora); se
    puede pasar explicito para reprocesos."""
    resumen = (
        df_silver.groupby(COLUMNAS_GRANULARIDAD, dropna=False)
        .agg(cantidadEventos=("auth_event_id", "count"))
        .reset_index()
    )
    resumen["fecha_snapshot"] = (fecha_snapshot or datetime.now(timezone.utc)).date()
    return resumen


# --- Nombrado y escritura del resultado -------------------------------------

def nombre_snapshot_semanal(fecha: datetime) -> str:
    """Nombre del parquet de snapshot semanal (se guarda los domingos), con
    la semana ISO del año en la que cae `fecha`."""
    anio, semana, _ = fecha.isocalendar()
    return f"LDAP/autenticacion_resumen_{semana:02d}_{anio}.parquet"


def escribir_resumen(df_resumen: pd.DataFrame, nombre_blob: str = TABLA_AUTENTICACION_GOLD_BLOB) -> None:
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
def autenticacion_silver_a_gold(timer: func.TimerRequest):
    """Corre una vez por dia a la 1am, una hora despues de que
    bp_autenticacion_silver corre a las 00:00, y recalcula el resumen
    agregado de gold desde cero a partir de la tabla de silver completa."""
    logging.info("Corrida diaria de autenticacion silver->gold.")

    fecha_snapshot = datetime.now(timezone.utc)

    df_silver = leer_tabla(get_silver_blob_client())
    resumen = calcular_resumen(df_silver, fecha_snapshot=fecha_snapshot)
    escribir_resumen(resumen)

    if fecha_snapshot.weekday() == 6:  # domingo
        nombre_blob = nombre_snapshot_semanal(fecha_snapshot)
        escribir_resumen(resumen, nombre_blob=nombre_blob)
        logging.info(f"Es domingo: snapshot semanal guardado en {nombre_blob}")

    logging.info(f"Resumen gold de autenticacion actualizado: {len(resumen)} filas.")
