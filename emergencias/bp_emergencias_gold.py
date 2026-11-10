"""Capa gold de emergencias y seguridad.

Timer trigger diario que agrega la tabla completa de silver/Emergencias y
Seguridad/emergencias.parquet, y guarda el resultado en gold/Emergencias y
Seguridad/emergencias_resumen.parquet.

Grano [estado_actual, prioridad, categoria, causaTipo, afectaCirculacion],
pensada para responder las preguntas de negocio del CONTRATO_EMERGENCIAS
(seccion 5, "Metricas que salen con esto"):
  1. Criticidad afectada por evento climatico -> filtrar causaTipo =
     EVENTO_CLIMATICO; cantidadEmergencias vs criticidadAfectada (cuantas
     tienen prioridad != prioridadOriginal).
  2. Emergencias a causa de un reclamo -> filtrar causaTipo = RECLAMO.
  3. Servicios enviados preventivamente -> filtrar categoria = PREVENTIVA.
  4. Emergencias por incidentes de transito -> filtrar afectaCirculacion.
  5. Emergencias por incidente vial -> filtrar causaTipo = INCIDENTE_VIAL.
  6. Emergencias por evento ciudadano -> filtrar causaTipo = RESERVA.
Mas las 3 preguntas originales: cantidad por fase (estado_actual), por
gravedad (prioridad), y rapidez de respuesta (tiempoPromRespuesta
Despacho/Lugar, en minutos desde fecha_creada).

tiempoPromRespuesta es "por prioridad", sin filtrar por el resto de la
granularidad — si se calculara dentro de cada combinacion completa, quedaria
diluido (ej. el promedio de las ALTA que hoy estan en CERRADA, no el de
todas las ALTA). Por eso se calcula aparte, una vez por prioridad, y se
repite en todas las filas de esa prioridad — asi cada fila sigue sirviendo
para esos pedidos sin que ninguno quede aproximado.

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

from .bp_emergencias_silver import leer_tabla
from .bp_emergencias_silver import get_blob_client as get_silver_blob_client

bp = func.Blueprint()

GOLD_CONTAINER_NAME = "gold"
TABLA_EMERGENCIAS_GOLD_BLOB = "Emergencias y Seguridad/emergencias_resumen.parquet"

COLUMNAS_GRANULARIDAD = ["estado_actual", "prioridad", "categoria", "causaTipo", "afectaCirculacion"]


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


def _get_gold_blob_client(nombre_blob: str = TABLA_EMERGENCIAS_GOLD_BLOB):
    return _get_gold_container_client().get_blob_client(nombre_blob)


# --- Agregacion --------------------------------------------------------------

def calcular_resumen(df_silver: pd.DataFrame, fecha_snapshot: datetime | None = None) -> pd.DataFrame:
    """Una fila por combinacion de estado_actual/prioridad con la cantidad
    de emergencias, mas el tiempo promedio de respuesta (minutos desde
    fecha_creada hasta fecha_despachada / fecha_en_lugar) calculado por
    prioridad sola y repetido en cada fila de esa prioridad. fecha_snapshot
    marca el momento en que se calculo este resumen (por defecto, hoy); se
    puede pasar explicito para reprocesos."""
    df = df_silver.copy()
    df["_min_hasta_despachada"] = (df["fecha_despachada"] - df["fecha_creada"]).dt.total_seconds() / 60
    df["_min_hasta_en_lugar"] = (df["fecha_en_lugar"] - df["fecha_creada"]).dt.total_seconds() / 60
    # Caso 1 (criticidad afectada por clima): prioridadOriginal solo viene
    # completo cuando causaTipo = EVENTO_CLIMATICO, asi que comparar no suma
    # falsos positivos en el resto de las emergencias (prioridadOriginal null).
    df["_criticidad_afectada"] = df["prioridadOriginal"].notna() & (df["prioridad"] != df["prioridadOriginal"])

    conteo = (
        df.groupby(COLUMNAS_GRANULARIDAD, dropna=False)
        .agg(
            cantidadEmergencias=("emergenciaId", "count"),
            criticidadAfectada=("_criticidad_afectada", "sum"),
        )
        .reset_index()
    )

    tiempos_por_prioridad = (
        df.groupby("prioridad", dropna=False)
        .agg(
            tiempoPromRespuestaDespacho=("_min_hasta_despachada", "mean"),
            tiempoPromRespuestaLugar=("_min_hasta_en_lugar", "mean"),
        )
        .reset_index()
    )

    resumen = conteo.merge(tiempos_por_prioridad, on="prioridad", how="left")
    resumen["fecha_snapshot"] = (fecha_snapshot or datetime.now(timezone.utc)).date()
    return resumen


# --- Nombrado y escritura del resultado -------------------------------------

def nombre_snapshot_semanal(fecha: datetime) -> str:
    """Nombre del parquet de snapshot semanal (se guarda los domingos), con
    la semana ISO del año en la que cae `fecha`."""
    anio, semana, _ = fecha.isocalendar()
    return f"Emergencias y Seguridad/emergencias_resumen_{semana:02d}_{anio}.parquet"


def escribir_resumen(df_resumen: pd.DataFrame, nombre_blob: str = TABLA_EMERGENCIAS_GOLD_BLOB) -> None:
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
def emergencias_silver_a_gold(timer: func.TimerRequest):
    """Corre una vez por dia a la 1am, una hora despues de que
    bp_emergencias_silver corre a las 00:00, y recalcula el resumen
    agregado de gold desde cero a partir de la tabla de silver completa."""
    logging.info("Corrida diaria de emergencias silver->gold.")

    fecha_snapshot = datetime.now(timezone.utc)

    df_silver = leer_tabla(get_silver_blob_client())
    resumen = calcular_resumen(df_silver, fecha_snapshot=fecha_snapshot)
    escribir_resumen(resumen)

    if fecha_snapshot.weekday() == 6:  # domingo
        nombre_blob = nombre_snapshot_semanal(fecha_snapshot)
        escribir_resumen(resumen, nombre_blob=nombre_blob)
        logging.info(f"Es domingo: snapshot semanal guardado en {nombre_blob}")

    logging.info(f"Resumen gold de emergencias actualizado: {len(resumen)} filas.")
