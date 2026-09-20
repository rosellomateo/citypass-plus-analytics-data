"""Capa gold de gestion de residuos inteligente.

Timer trigger diario que agrega la tabla completa de silver/Gestion de
Residuos Inteligente/alertas.parquet por zona/tipoAlerta/prioridad/
rangoNivelLlenado, y guarda el resultado en gold/Gestion de Residuos
Inteligente/alertas_resumen.parquet.

Al igual que en gold de reclamos/movilidad, aca no hay incremental/checkpoint:
cada corrida recalcula el resumen entero a partir de la tabla de silver
actual. Los domingos, ademas de sobreescribir la tabla principal, se guarda
una copia con la semana ISO en el nombre (snapshot historico).
"""
import azure.functions as func
import logging
import os
from datetime import datetime, timezone
from io import BytesIO

import pandas as pd
from azure.core.exceptions import ResourceExistsError
from azure.storage.blob import BlobServiceClient

from bp_residuos_silver import leer_tabla
from bp_residuos_silver import get_blob_client as get_silver_blob_client

bp = func.Blueprint()

GOLD_CONTAINER_NAME = "gold"
TABLA_RESIDUOS_GOLD_BLOB = "Gestion de Residuos Inteligente/alertas_resumen.parquet"

COLUMNAS_GRANULARIDAD = ["zona", "tipoAlerta", "prioridad", "rangoNivelLlenado"]


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


def _get_gold_blob_client(nombre_blob: str = TABLA_RESIDUOS_GOLD_BLOB):
    return _get_gold_container_client().get_blob_client(nombre_blob)


# --- Agregacion --------------------------------------------------------------

def _bucket_nivel_llenado(nivel):
    """Bucketiza nivelLlenado (0-100) en franjas de 20. None (queda como
    NaN en la tabla) para las alertas de FALLA_SENSOR, que no traen nivel."""
    if pd.isna(nivel):
        return None
    if nivel < 40:
        return "0-40"
    if nivel < 60:
        return "40-60"
    if nivel < 80:
        return "60-80"
    return "80-100"


def calcular_resumen(df_silver: pd.DataFrame, fecha_snapshot: datetime | None = None) -> pd.DataFrame:
    """Una fila por combinacion de zona/tipoAlerta/prioridad/rangoNivelLlenado,
    con la cantidad de alertas, cuantas se resolvieron y el promedio de
    minutos hasta la resolucion (solo sobre las resueltas: .mean() ya
    ignora los NaN de las que siguen abiertas). fecha_snapshot marca el
    momento en que se calculo este resumen (por defecto, hoy); se puede
    pasar explicito para reprocesos."""
    df = df_silver.copy()
    df["rangoNivelLlenado"] = df["nivelLlenado"].apply(_bucket_nivel_llenado)

    resumen = (
        df.groupby(COLUMNAS_GRANULARIDAD, dropna=False)
        .agg(
            cantidadAlertas=("alertaId", "count"),
            cantidadResueltas=("resuelta", "sum"),
            tiempoPromResolucion=("tiempo_resolucion_minutos", "mean"),
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
    return f"Gestion de Residuos Inteligente/alertas_resumen_{semana:02d}_{anio}.parquet"


def escribir_resumen(df_resumen: pd.DataFrame, nombre_blob: str = TABLA_RESIDUOS_GOLD_BLOB) -> None:
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
def residuos_silver_a_gold(timer: func.TimerRequest):
    """Corre una vez por dia a la 1am, una hora despues de que
    bp_residuos_silver corre a las 00:00, y recalcula el resumen agregado
    de gold desde cero a partir de la tabla de silver completa."""
    logging.info("Corrida diaria de residuos silver->gold.")

    fecha_snapshot = datetime.now(timezone.utc)

    df_silver = leer_tabla(get_silver_blob_client())
    resumen = calcular_resumen(df_silver, fecha_snapshot=fecha_snapshot)
    escribir_resumen(resumen)

    if fecha_snapshot.weekday() == 6:  # domingo
        nombre_blob = nombre_snapshot_semanal(fecha_snapshot)
        escribir_resumen(resumen, nombre_blob=nombre_blob)
        logging.info(f"Es domingo: snapshot semanal guardado en {nombre_blob}")

    logging.info(f"Resumen gold de residuos actualizado: {len(resumen)} filas.")
