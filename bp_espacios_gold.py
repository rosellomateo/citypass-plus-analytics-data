"""Capa gold de espacios publicos y cultura.

Timer trigger diario que agrega la tabla completa de silver/Espacios
Publicos y Cultura/reservas.parquet por recursoId (un espacio fisico o un
evento comunitario, segun tipoReserva), y guarda el resultado en
gold/Espacios Publicos y Cultura/reservas_resumen.parquet.

Una sola tabla, grano recursoId, pensada para responder 3 preguntas de
negocio sin necesitar tablas separadas:
  1. Uso efectivo vs. turnos cancelados por parque/predio -> filtrar
     tipoReserva=ESPACIO y comparar cantidadConfirmadas vs cantidadCanceladas.
  2. Distribucion de asistencias por tematica -> filtrar tipoReserva=EVENTO
     y agrupar/sumar inscriptos por categoria (CULTURAL/DEPORTIVO/RECREATIVO).
  3. Detalle evento por evento (inscriptos, cupoMaximo, % ocupacion) -> cada
     fila con tipoReserva=EVENTO ya es un evento.

inscriptos y pctOcupacion solo cuentan reservas CONFIRMADA (una cancelada
no debe sumar como inscripto real ni ocupar cupo; una PENDIENTE tampoco,
hasta que se confirme).

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

from bp_espacios_silver import leer_tabla
from bp_espacios_silver import get_blob_client as get_silver_blob_client

bp = func.Blueprint()

GOLD_CONTAINER_NAME = "gold"
TABLA_ESPACIOS_GOLD_BLOB = "Espacios Publicos y Cultura/reservas_resumen.parquet"

COLUMNAS_GRANULARIDAD = ["recursoId", "tipoReserva", "categoria", "zona", "cupoMaximo"]


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


def _get_gold_blob_client(nombre_blob: str = TABLA_ESPACIOS_GOLD_BLOB):
    return _get_gold_container_client().get_blob_client(nombre_blob)


# --- Agregacion --------------------------------------------------------------

def _pct_ocupacion(fila: pd.Series):
    """inscriptos / cupoMaximo, en porcentaje. None si el recurso no tiene
    cupoMaximo (caso ESPACIO) o si es 0."""
    cupo = fila["cupoMaximo"]
    if pd.isna(cupo) or cupo <= 0:
        return None
    return round(fila["inscriptos"] / cupo * 100, 2)


def calcular_resumen(df_silver: pd.DataFrame, fecha_snapshot: datetime | None = None) -> pd.DataFrame:
    """Una fila por recursoId, con cuantas reservas tiene confirmadas,
    canceladas y en total, mas inscriptos (personas de las reservas
    CONFIRMADA) y % de ocupacion sobre cupoMaximo (solo tiene sentido para
    EVENTO; en ESPACIO queda null). fecha_snapshot marca el momento en que
    se calculo este resumen (por defecto, hoy); se puede pasar explicito
    para reprocesos."""
    df = df_silver.copy()
    df["_personas_confirmadas"] = df["cantidadPersonas"].where(df["confirmada"], 0)

    resumen = (
        df.groupby(COLUMNAS_GRANULARIDAD, dropna=False)
        .agg(
            cantidadTotal=("reservaId", "count"),
            cantidadConfirmadas=("confirmada", "sum"),
            cantidadCanceladas=("cancelada", "sum"),
            inscriptos=("_personas_confirmadas", "sum"),
        )
        .reset_index()
    )
    resumen["pctOcupacion"] = resumen.apply(_pct_ocupacion, axis=1)
    resumen["fecha_snapshot"] = (fecha_snapshot or datetime.now(timezone.utc)).date()
    return resumen


# --- Nombrado y escritura del resultado -------------------------------------

def nombre_snapshot_semanal(fecha: datetime) -> str:
    """Nombre del parquet de snapshot semanal (se guarda los domingos), con
    la semana ISO del año en la que cae `fecha`."""
    anio, semana, _ = fecha.isocalendar()
    return f"Espacios Publicos y Cultura/reservas_resumen_{semana:02d}_{anio}.parquet"


def escribir_resumen(df_resumen: pd.DataFrame, nombre_blob: str = TABLA_ESPACIOS_GOLD_BLOB) -> None:
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
def espacios_silver_a_gold(timer: func.TimerRequest):
    """Corre una vez por dia a la 1am, una hora despues de que
    bp_espacios_silver corre a las 00:00, y recalcula el resumen agregado
    de gold desde cero a partir de la tabla de silver completa."""
    logging.info("Corrida diaria de espacios silver->gold.")

    fecha_snapshot = datetime.now(timezone.utc)

    df_silver = leer_tabla(get_silver_blob_client())
    resumen = calcular_resumen(df_silver, fecha_snapshot=fecha_snapshot)
    escribir_resumen(resumen)

    if fecha_snapshot.weekday() == 6:  # domingo
        nombre_blob = nombre_snapshot_semanal(fecha_snapshot)
        escribir_resumen(resumen, nombre_blob=nombre_blob)
        logging.info(f"Es domingo: snapshot semanal guardado en {nombre_blob}")

    logging.info(f"Resumen gold de espacios actualizado: {len(resumen)} filas.")
