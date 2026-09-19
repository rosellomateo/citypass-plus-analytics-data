"""Capa gold de movilidad urbana.

Timer trigger diario que agrega la tabla completa de silver/Movilidad Urbana/
viajes.parquet por fechaInicio/estacionInicio/duracionViaje, y guarda el
resultado en gold/Movilidad Urbana/viajes_resumen.parquet.

Los viajes en curso (sin ViajeTerminado, duracion_minutos null) se excluyen
de esta agregacion porque no tienen una duracion para bucketizar. Al igual
que en gold de reclamos, aca no hay incremental/checkpoint: cada corrida
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

from bp_movilidad_silver import leer_tabla
from bp_movilidad_silver import get_blob_client as get_silver_blob_client

bp = func.Blueprint()

GOLD_CONTAINER_NAME = "gold"
TABLA_MOVILIDAD_GOLD_BLOB = "Movilidad Urbana/viajes_resumen.parquet"

COLUMNAS_GRANULARIDAD = ["fechaInicio", "estacionInicio", "duracionViaje"]


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


def _get_gold_blob_client(nombre_blob: str = TABLA_MOVILIDAD_GOLD_BLOB):
    return _get_gold_container_client().get_blob_client(nombre_blob)


# --- Agregacion --------------------------------------------------------------

def _bucket_duracion(minutos):
    """Bucketiza duracion_minutos en las 4 franjas pedidas. None si el viaje
    todavia no tiene duracion (en curso)."""
    if pd.isna(minutos):
        return None
    if minutos < 15:
        return "<15min"
    if minutos < 30:
        return "15-30min"
    if minutos < 60:
        return "30-60min"
    return ">1hs"


def calcular_resumen(df_silver: pd.DataFrame, fecha_snapshot: datetime | None = None) -> pd.DataFrame:
    """Una fila por combinacion de fechaInicio/estacionInicio/duracionViaje,
    con la cantidad de viajes, la suma y el promedio de duracion (en
    minutos). Excluye los viajes en curso (duracion_minutos null).
    fecha_snapshot marca el momento en que se calculo este resumen (por
    defecto, ahora); se puede pasar explicito para reprocesos."""
    df = df_silver[df_silver["duracion_minutos"].notna()].copy()

    df["fechaInicio"] = df["horaInicio"].dt.date
    df["duracionViaje"] = df["duracion_minutos"].apply(_bucket_duracion)

    resumen = (
        df.groupby(COLUMNAS_GRANULARIDAD, dropna=False)
        .agg(
            cantidadViajes=("viajeId", "count"),
            duracionTotalViajes=("duracion_minutos", "sum"),
            promDuracion=("duracion_minutos", "mean"),
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
    return f"Movilidad Urbana/viajes_resumen_{semana:02d}_{anio}.parquet"


def escribir_resumen(df_resumen: pd.DataFrame, nombre_blob: str = TABLA_MOVILIDAD_GOLD_BLOB) -> None:
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
def movilidad_silver_a_gold(timer: func.TimerRequest):
    """Corre una vez por dia a la 1am, una hora despues de que
    bp_movilidad_silver corre a las 00:00, y recalcula el resumen agregado
    de gold desde cero a partir de la tabla de silver completa."""
    logging.info("Corrida diaria de movilidad silver->gold.")

    fecha_snapshot = datetime.now(timezone.utc)

    df_silver = leer_tabla(get_silver_blob_client())
    resumen = calcular_resumen(df_silver, fecha_snapshot=fecha_snapshot)
    escribir_resumen(resumen)

    if fecha_snapshot.weekday() == 6:  # domingo
        nombre_blob = nombre_snapshot_semanal(fecha_snapshot)
        escribir_resumen(resumen, nombre_blob=nombre_blob)
        logging.info(f"Es domingo: snapshot semanal guardado en {nombre_blob}")

    logging.info(f"Resumen gold de movilidad actualizado: {len(resumen)} filas.")
