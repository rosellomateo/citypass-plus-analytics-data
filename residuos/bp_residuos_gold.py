"""Capa gold de gestion de residuos inteligente (intervenciones, Grupo 6).

Timer trigger diario que agrega la tabla completa de silver/Gestion de
Residuos Inteligente/intervenciones.parquet y guarda el resultado en
gold/Gestion de Residuos Inteligente/intervenciones_resumen.parquet.

Grano [estado_actual, causaProgramacion, causaTipoAfectacion, barrio,
origenProgramacion], pensado para las metricas de Residuos de los 6 casos
de uso (MODELADO_EVENTOS_PARTE2.md, 4.3):
  1. Cuadrillas afectadas por clima -> causaTipoAfectacion = EVENTO_CLIMATICO
     (cantidadAfectadas / cantidadCanceladas)
  2. Cuadrillas por reclamo, por barrio -> causaProgramacion = RECLAMO, agrupar por barrio
  3. Cuadrillas para el evento masivo -> causaProgramacion = EVENTO_PUBLICO
  4. Cuadrillas afectadas por incidentes -> causaTipoAfectacion = INCIDENTE_VIAL
  5. Por accidente vs. por alertas normales -> origenProgramacion
     EMERGENCIA (emergenciaId no nulo) vs. ALERTA (alertaId no nulo)
  6. Cuadrillas cerca del espacio reservado -> causaProgramacion = RESERVA

causaProgramacion es la causa de la PROGRAMACION y causaTipoAfectacion la de
la AFECTACION/CANCELACION (ver bp_residuos_silver.py; en silver esta columna
todavia se llama causaTipo -- gold la renombra al agrupar, para que no se
confunda con causaTipoAfectacion). camionesDistintos es por fila: no se
puede sumar entre filas (el mismo camion puede estar en varias).

Al igual que en gold de reclamos/emergencias, aca no hay incremental/
checkpoint: cada corrida recalcula el resumen entero a partir de la tabla de
silver actual. Los domingos, ademas de sobreescribir la tabla principal, se
guarda una copia con la semana ISO en el nombre (snapshot historico).
"""
import azure.functions as func
import logging
import os
from datetime import datetime, timezone
from io import BytesIO

import pandas as pd
from azure.core.exceptions import ResourceExistsError
from azure.storage.blob import BlobServiceClient

from .bp_residuos_silver import leer_tabla
from .bp_residuos_silver import get_blob_client as get_silver_blob_client

bp = func.Blueprint()

GOLD_CONTAINER_NAME = "gold"
TABLA_RESIDUOS_GOLD_BLOB = "Gestion de Residuos Inteligente/intervenciones_resumen.parquet"

COLUMNAS_GRANULARIDAD = ["estado_actual", "causaProgramacion", "causaTipoAfectacion", "barrio", "origenProgramacion"]


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

def _origen_programacion(df: pd.DataFrame) -> pd.Series:
    """Caso 5: EMERGENCIA si la intervencion se programo por una emergencia
    (emergenciaId no nulo), ALERTA si fue por una alerta propia de Residuos
    (alertaId no nulo), y NINGUNO si no tiene ninguna de las dos."""
    origen = pd.Series("NINGUNO", index=df.index, dtype="object")
    origen[df["alertaId"].notna()] = "ALERTA"
    origen[df["emergenciaId"].notna()] = "EMERGENCIA"
    return origen


def calcular_resumen(df_silver: pd.DataFrame, fecha_snapshot: datetime | None = None) -> pd.DataFrame:
    """Una fila por combinacion de la granularidad, con la cantidad de
    intervenciones, cuantas quedaron afectadas / canceladas / completadas y
    cuantos camiones distintos participaron. fecha_snapshot marca el momento
    en que se calculo este resumen (por defecto, hoy); se puede pasar
    explicito para reprocesos."""
    df = df_silver.copy()
    for col in ("causaTipo", "causaTipoAfectacion", "barrio", "emergenciaId", "alertaId", "camionId"):
        if col not in df.columns:
            df[col] = None
    for col in ("afectada", "cancelada", "completada"):
        if col not in df.columns:
            df[col] = False
        df[col] = df[col].fillna(False).astype(bool)
    df["origenProgramacion"] = _origen_programacion(df)
    # En silver esta columna se llama causaTipo; gold la renombra para que no
    # se confunda con causaTipoAfectacion (ver modulo docstring).
    df = df.rename(columns={"causaTipo": "causaProgramacion"})

    resumen = (
        df.groupby(COLUMNAS_GRANULARIDAD, dropna=False)
        .agg(
            cantidadIntervenciones=("intervencionId", "count"),
            cantidadAfectadas=("afectada", "sum"),
            cantidadCanceladas=("cancelada", "sum"),
            cantidadCompletadas=("completada", "sum"),
            camionesDistintos=("camionId", "nunique"),
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
    return f"Gestion de Residuos Inteligente/intervenciones_resumen_{semana:02d}_{anio}.parquet"


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
