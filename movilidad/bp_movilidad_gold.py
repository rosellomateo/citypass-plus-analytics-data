"""Capa gold de movilidad urbana.

Timer trigger diario que combina las DOS tablas de silver/Movilidad Urbana/
(viajes.parquet y estaciones_afectadas.parquet) en un solo resumen, agrupado
por `causaTipo` -- el enum que comparten los dos eventos (EstacionUrbanaAfectada
y ViajeFinalizado) y que unifica el grano aunque las dos tablas tengan una
entidad de base distinta (una estacion afectada vs. un viaje). El resultado
se guarda en gold/Movilidad Urbana/movilidad_resumen.parquet.

Cubre las 6 metricas de CONTRATO_MOVILIDAD.md seccion 5, todas filtrando por
causaTipo sobre esta misma tabla:
  1. Bloqueadas por evento climatico -> causaTipo=EVENTO_CLIMATICO,
     columna cantidadBloqueadas
  2. Afectadas por reclamos -> causaTipo=RECLAMO, columna cantidadAfectaciones
  3. Afectacion preventiva anunciada -> causaTipo=EVENTO_PUBLICO, columna
     cantidadPreventivas (afectacionDesde > occurredAt)
  4. Afectadas por incidentes -> causaTipo=INCIDENTE_VIAL, columna
     cantidadAfectaciones
  5. Viajes interrumpidos por incidente -> causaTipo=INCIDENTE_VIAL, columna
     cantidadViajesInterrumpidos, comparado contra la fila causaTipo=NORMAL
     (viajes finalizados sin incidente; el resto de sus columnas queda en 0,
     porque esa fila no corresponde a ninguna afectacion de estacion)
  6. Afectadas por eventos de ciudadanos -> causaTipo=RESERVA, columna
     cantidadAfectaciones

estacionesAfectadasDistintas cuenta estaciones unicas (no cuantas veces se
afectaron), para los casos que piden "estaciones distintas" en vez de
"cantidad de afectaciones".

Al igual que en el resto de las tablas gold, aca no hay incremental/
checkpoint: cada corrida recalcula el resumen entero a partir de las tablas
de silver actuales. Los domingos, ademas de sobreescribir la tabla
principal, se guarda una copia con la semana ISO en el nombre (snapshot
historico).
"""
import azure.functions as func
import logging
import os
from datetime import datetime, timezone
from io import BytesIO

import pandas as pd
from azure.core.exceptions import ResourceExistsError
from azure.storage.blob import BlobServiceClient

from .bp_movilidad_silver import leer_tabla, leer_tabla_estaciones
from .bp_movilidad_silver import get_blob_client as get_silver_blob_client_viajes
from .bp_movilidad_silver import get_blob_client_estaciones as get_silver_blob_client_estaciones

bp = func.Blueprint()

GOLD_CONTAINER_NAME = "gold"
TABLA_MOVILIDAD_GOLD_BLOB = "Movilidad Urbana/movilidad_resumen.parquet"

COLUMNA_GRANULARIDAD = "causaTipo"


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

COLUMNAS_CONTADOR = [
    "cantidadAfectaciones",
    "estacionesAfectadasDistintas",
    "cantidadBloqueadas",
    "cantidadPreventivas",
    "cantidadRestauradas",
    "cantidadViajesInterrumpidos",
]


def _resumen_estaciones(df_estaciones: pd.DataFrame) -> pd.DataFrame:
    df = df_estaciones.copy()
    df["_bloqueada"] = df["tipoAfectacion"] == "BLOQUEADA"
    # Afectacion anunciada preventivamente: se publica con afectacionDesde a
    # futuro respecto de cuando se publico el propio evento.
    df["_preventiva"] = df["afectacionDesde"] > df["occurredAt"]

    return (
        df.groupby(COLUMNA_GRANULARIDAD, dropna=False)
        .agg(
            cantidadAfectaciones=("afectacionId", "count"),
            estacionesAfectadasDistintas=("estacionId", "nunique"),
            cantidadBloqueadas=("_bloqueada", "sum"),
            cantidadPreventivas=("_preventiva", "sum"),
            cantidadRestauradas=("restaurada", "sum"),
        )
        .reset_index()
    )


CAUSA_TIPO_NORMAL = "NORMAL"


def _resumen_viajes(df_viajes: pd.DataFrame) -> pd.DataFrame:
    # Los viajes sin interrumpir no traen causaTipo (null): se agrupan bajo
    # un valor propio, NORMAL, en vez de dejarlos como una fila null -- asi
    # tienen su propia fila legible en vez de "fusionarse" con el resto.
    df = df_viajes.copy()
    df[COLUMNA_GRANULARIDAD] = df[COLUMNA_GRANULARIDAD].fillna(CAUSA_TIPO_NORMAL)
    return (
        df.groupby(COLUMNA_GRANULARIDAD, dropna=False)
        .agg(cantidadViajesInterrumpidos=("viajeId", "count"))
        .reset_index()
    )


def calcular_resumen(
    df_viajes: pd.DataFrame, df_estaciones: pd.DataFrame, fecha_snapshot: datetime | None = None,
) -> pd.DataFrame:
    """Una fila por causaTipo, combinando estaciones_afectadas.parquet y
    viajes.parquet, mas una fila causaTipo=NORMAL con los viajes finalizados
    sin incidente (el resto de sus columnas, que vienen de estaciones, queda
    en 0: esa fila no tiene afectaciones). fecha_snapshot marca el momento
    en que se calculo este resumen (por defecto, ahora); se puede pasar
    explicito para reprocesos."""
    resumen = pd.merge(
        _resumen_estaciones(df_estaciones), _resumen_viajes(df_viajes),
        on=COLUMNA_GRANULARIDAD, how="outer",
    )
    for col in COLUMNAS_CONTADOR:
        resumen[col] = resumen[col].fillna(0).astype(int)

    resumen["fecha_snapshot"] = (fecha_snapshot or datetime.now(timezone.utc)).date()
    return resumen


# --- Nombrado y escritura del resultado -------------------------------------

def nombre_snapshot_semanal(fecha: datetime) -> str:
    """Nombre del parquet de snapshot semanal (se guarda los domingos), con
    la semana ISO del año en la que cae `fecha`."""
    anio, semana, _ = fecha.isocalendar()
    return f"Movilidad Urbana/movilidad_resumen_{semana:02d}_{anio}.parquet"


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
    bp_movilidad_silver corre a las 00:00, y recalcula el resumen combinado
    de gold desde cero a partir de las dos tablas de silver completas."""
    logging.info("Corrida diaria de movilidad silver->gold.")

    fecha_snapshot = datetime.now(timezone.utc)

    df_viajes = leer_tabla(get_silver_blob_client_viajes())
    df_estaciones = leer_tabla_estaciones(get_silver_blob_client_estaciones())
    resumen = calcular_resumen(df_viajes, df_estaciones, fecha_snapshot=fecha_snapshot)
    escribir_resumen(resumen)

    if fecha_snapshot.weekday() == 6:  # domingo
        nombre_blob = nombre_snapshot_semanal(fecha_snapshot)
        escribir_resumen(resumen, nombre_blob=nombre_blob)
        logging.info(f"Es domingo: snapshot semanal guardado en {nombre_blob}")

    logging.info(f"Resumen gold de movilidad actualizado: {len(resumen)} filas.")
