"""Capa gold de espacios publicos y cultura.

Timer trigger diario que agrega la tabla completa de silver/Espacios
Publicos y Cultura/actividades.parquet por estado_actual/categoria/
causaTipo/organizadorTipo/barrio, y guarda el resultado en
gold/Espacios Publicos y Cultura/actividades_resumen.parquet.

NOTA (14/nov): el flujo original de este modulo era Reservas (gold sobre
reservas.parquet, por recursoId), pero nunca se llego a usar en produccion
y se decidio no usarlo -- se borraron los blobs de silver/gold que tenia.
Todo el modulo quedo centrado en Actividades.

Cubre las 6 metricas de CONTRATO_ESPACIOS.md seccion 5:
  1. Culturales canceladas por clima -> estado_actual=CANCELADA,
     causaTipo=EVENTO_CLIMATICO, categoria=CULTURAL -> cantidadActividades
  2. Afectadas por reclamo -> causaTipo=RECLAMO -> cantidadActividades
  3. Eventos masivos creados -> sumar cantidadMasivas (capacidad >= 500)
  4. Afectadas por incidente vial -> causaTipo=INCIDENTE_VIAL ->
     cantidadActividades (actividades DISTINTAS)
  5. Modificadas por accidente vial -> sumar totalVecesAccidenteVial
     (ACCIDENTE_VIAL es un causaTipo DISTINTO de INCIDENTE_VIAL del caso 4,
     no un typo -- confirmado con datos reales: 18 eventos INCIDENTE_VIAL
     vs 15 ACCIDENTE_VIAL)
  6. Ciudadanos vs. ciudad -> agrupar por organizadorTipo
Las 3 primeras se obtienen filtrando/sumando esta misma tabla agrupada por
estado_actual/categoria/causaTipo/organizadorTipo, igual que el resto de
los dominios (ver bp_emergencias_gold.py).

Ademas se agrupa por barrio (derivado en silver por point-in-polygon contra
lat/lng, ver espacios/_barrios.py -- el evento no trae barrio) para poder
ver la cantidad de actividades por barrio y categoria.

Al igual que en el resto de las tablas gold, aca no hay incremental/
checkpoint: cada corrida recalcula el resumen entero a partir de la tabla
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

from .bp_espacios_silver import leer_tabla_actividades
from .bp_espacios_silver import get_blob_client_actividades as get_silver_blob_client_actividades

bp = func.Blueprint()

GOLD_CONTAINER_NAME = "gold"
TABLA_ACTIVIDADES_GOLD_BLOB = "Espacios Publicos y Cultura/actividades_resumen.parquet"

COLUMNAS_GRANULARIDAD_ACTIVIDAD = ["estado_actual", "categoria", "causaTipo", "organizadorTipo", "barrio"]


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


def _get_gold_blob_client_actividades(nombre_blob: str = TABLA_ACTIVIDADES_GOLD_BLOB):
    return _get_gold_container_client().get_blob_client(nombre_blob)


# --- Agregacion --------------------------------------------------------------

def calcular_resumen_actividades(df_silver: pd.DataFrame, fecha_snapshot: datetime | None = None) -> pd.DataFrame:
    """Una fila por combinacion de estado_actual/categoria/causaTipo/
    organizadorTipo/barrio, con la cantidad de actividades, cuantas son masivas
    (capacidad >= 500) y el total de modificaciones por cada causa (suma de
    los contadores veces_<causa> de silver). fecha_snapshot marca el
    momento en que se calculo este resumen (por defecto, ahora); se puede
    pasar explicito para reprocesos."""
    resumen = (
        df_silver.groupby(COLUMNAS_GRANULARIDAD_ACTIVIDAD, dropna=False)
        .agg(
            cantidadActividades=("actividadId", "count"),
            cantidadMasivas=("masivo", "sum"),
            totalVecesEventoClimatico=("veces_evento_climatico", "sum"),
            totalVecesReclamo=("veces_reclamo", "sum"),
            totalVecesIncidenteVial=("veces_incidente_vial", "sum"),
            totalVecesAccidenteVial=("veces_accidente_vial", "sum"),
        )
        .reset_index()
    )
    resumen["fecha_snapshot"] = (fecha_snapshot or datetime.now(timezone.utc)).date()
    return resumen


# --- Nombrado y escritura del resultado -------------------------------------

def nombre_snapshot_semanal_actividades(fecha: datetime) -> str:
    """Nombre del parquet de snapshot semanal (se guarda los domingos), con
    la semana ISO del año en la que cae `fecha`."""
    anio, semana, _ = fecha.isocalendar()
    return f"Espacios Publicos y Cultura/actividades_resumen_{semana:02d}_{anio}.parquet"


def escribir_resumen_actividades(df_resumen: pd.DataFrame, nombre_blob: str = TABLA_ACTIVIDADES_GOLD_BLOB) -> None:
    buffer = BytesIO()
    df_resumen.to_parquet(buffer, index=False)
    buffer.seek(0)
    _get_gold_blob_client_actividades(nombre_blob).upload_blob(buffer, overwrite=True)


# --- Trigger: corre 1 vez por dia, despues de silver ------------------------

@bp.timer_trigger(
    schedule="0 0 1 * * *",
    arg_name="timer",
    run_on_startup=False,
    use_monitor=False,
)
def espacios_actividades_silver_a_gold(timer: func.TimerRequest):
    """Corre una vez por dia a la 1am, una hora despues de que
    bp_espacios_silver corre a las 00:00, y recalcula el resumen agregado
    de actividades desde cero a partir de la tabla de silver completa."""
    logging.info("Corrida diaria de espacios (actividades) silver->gold.")

    fecha_snapshot = datetime.now(timezone.utc)

    df_silver = leer_tabla_actividades(get_silver_blob_client_actividades())
    resumen = calcular_resumen_actividades(df_silver, fecha_snapshot=fecha_snapshot)
    escribir_resumen_actividades(resumen)

    if fecha_snapshot.weekday() == 6:  # domingo
        nombre_blob = nombre_snapshot_semanal_actividades(fecha_snapshot)
        escribir_resumen_actividades(resumen, nombre_blob=nombre_blob)
        logging.info(f"Es domingo: snapshot semanal guardado en {nombre_blob}")

    logging.info(f"Resumen gold de actividades actualizado: {len(resumen)} filas.")
