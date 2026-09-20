"""Capa silver de espacios publicos y cultura.

Timer trigger diario que lee los eventos crudos de bronze/Espacios Publicos
y Cultura/ (ReservaCreada y ReservaActualizada) y arma una tabla en
silver/Espacios Publicos y Cultura/reservas.parquet: una fila por reserva
(ya sea de un ESPACIO fisico o de inscripcion a un EVENTO comunitario).

A diferencia de reclamos, aca la maquina de estados es simple y de un solo
salto: PENDIENTE (al crear) -> CONFIRMADA | CANCELADA (unica transicion
posible, ver README del modulo). Por eso no hace falta una columna de fecha
por estado: alcanza con estado_actual + fecha de esa actualizacion.

Procesa solo lo nuevo desde la ultima corrida (checkpoint mas abajo), mismo
patron que bp_reclamos_silver.py / bp_movilidad_silver.py / bp_residuos_silver.py.
Para reprocesar todo el historico usar scripts/reprocesar_espacios_silver.py.
"""
import azure.functions as func
import logging
import json
import os
from datetime import datetime
from io import BytesIO

import pandas as pd
from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob import BlobServiceClient

bp = func.Blueprint()

# --- Configuracion: containers, paths de blobs ----------------------------

BRONZE_CONTAINER_NAME = "bronze"
SILVER_CONTAINER_NAME = "silver"
PREFIJO_ESPACIOS = "Espacios Publicos y Cultura/"
TABLA_RESERVAS_BLOB = "Espacios Publicos y Cultura/reservas.parquet"
CHECKPOINT_BLOB = "Espacios Publicos y Cultura/_checkpoint.json"

COLUMNAS_BASE = [
    "reservaId",
    "tipoReserva",
    "recursoId",
    "categoria",
    "zona",
    "fechaHora",
    "cantidadPersonas",
    "cupoMaximo",
    "ciudadanoId",
    "organizadorId",
    "fechaCreada",
]

COLUMNAS = [
    *COLUMNAS_BASE,
    "estado_actual",
    "fechaActualizacion",
    "motivo",
    "usuarioId",
    "confirmada",
    "cancelada",
]


# --- Helpers para armar/actualizar una fila de la tabla -------------------

def _iso_a_datetime(iso_str):
    """fechaHora/createdAt/updatedAt vienen en ISO-8601; los pasamos a
    Timestamp UTC (pd.to_datetime tolera con/sin offset y el sufijo 'Z')."""
    if iso_str is None:
        return None
    return pd.to_datetime(iso_str, utc=True)


def dataframe_vacio() -> pd.DataFrame:
    """Tabla vacia con el esquema completo ya tipado, para arrancar desde
    cero o cuando todavia no existe el parquet en silver."""
    df = pd.DataFrame(columns=COLUMNAS)
    for col in ("fechaHora", "fechaCreada", "fechaActualizacion"):
        df[col] = pd.to_datetime(df[col], utc=True)
    for col in ("confirmada", "cancelada"):
        df[col] = df[col].astype(bool)
    return df


def _fila_nueva(reserva_id: str) -> dict:
    """Fila placeholder para un reservaId que todavia no tiene datos (se usa
    tanto para una ReservaCreada nueva como para una ReservaActualizada que
    llega antes que su ReservaCreada)."""
    fila = {col: None for col in COLUMNAS}
    fila["reservaId"] = reserva_id
    fila["confirmada"] = False
    fila["cancelada"] = False
    return fila


def _actualizar_derivados(df: pd.DataFrame, idx) -> None:
    """confirmada/cancelada se derivan directo de estado_actual, para poder
    filtrar sin comparar strings en los consumidores de la tabla."""
    estado = df.at[idx, "estado_actual"]
    df.at[idx, "confirmada"] = estado == "CONFIRMADA"
    df.at[idx, "cancelada"] = estado == "CANCELADA"


# --- Aplicar eventos (Creada / Actualizada) sobre el DataFrame ------------

def upsert_creada(df: pd.DataFrame, data: dict) -> pd.DataFrame:
    """Alta o completado de la fila a partir de una ReservaCreada. Si la
    reserva ya tiene fechaCreada cargada, es un evento duplicado: se ignora
    en vez de reprocesarlo. Si una ReservaActualizada ya llego antes (fuera
    de orden) y ya fijo un estado_actual, no lo pisa con PENDIENTE."""
    reserva_id = data.get("reservaId")

    if reserva_id in df["reservaId"].values:
        idx = df.index[df["reservaId"] == reserva_id][0]
        if pd.notna(df.at[idx, "fechaCreada"]):
            logging.info(f"ReservaCreada duplicada para {reserva_id}, se ignora.")
            return df
    else:
        df = pd.concat([df, pd.DataFrame([_fila_nueva(reserva_id)])], ignore_index=True)

    idx = df.index[df["reservaId"] == reserva_id][0]

    for col in COLUMNAS_BASE:
        if col != "reservaId" and col != "fechaHora" and col != "fechaCreada":
            df.at[idx, col] = data.get(col)

    df.at[idx, "fechaHora"] = _iso_a_datetime(data.get("fechaHora"))
    df.at[idx, "fechaCreada"] = _iso_a_datetime(data.get("createdAt"))

    if pd.isna(df.at[idx, "estado_actual"]):
        df.at[idx, "estado_actual"] = "PENDIENTE"

    _actualizar_derivados(df, idx)
    return df


def aplicar_actualizada(df: pd.DataFrame, data: dict) -> pd.DataFrame:
    """Aplica una ReservaActualizada (creando una fila placeholder si la
    ReservaCreada todavia no llego). El modelo solo admite una transicion
    (PENDIENTE -> CONFIRMADA|CANCELADA), asi que si la reserva ya tiene
    fechaActualizacion cargada, se trata como duplicado y se ignora."""
    reserva_id = data.get("reservaId")

    if reserva_id in df["reservaId"].values:
        idx = df.index[df["reservaId"] == reserva_id][0]
        if pd.notna(df.at[idx, "fechaActualizacion"]):
            logging.info(f"ReservaActualizada duplicada para {reserva_id}, se ignora.")
            return df
    else:
        df = pd.concat([df, pd.DataFrame([_fila_nueva(reserva_id)])], ignore_index=True)

    idx = df.index[df["reservaId"] == reserva_id][0]

    df.at[idx, "estado_actual"] = data.get("estadoNuevo")
    df.at[idx, "fechaActualizacion"] = _iso_a_datetime(data.get("updatedAt"))
    df.at[idx, "motivo"] = data.get("motivo")
    df.at[idx, "usuarioId"] = data.get("usuarioId")

    _actualizar_derivados(df, idx)
    return df


# --- Acceso a storage (bronze/silver) -------------------------------------

def _get_container_client(nombre_container: str):
    connection_string = os.environ["STORAGE_CONNECTION_STRING"]
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    return blob_service_client.get_container_client(nombre_container)


def get_bronze_container_client():
    return _get_container_client(BRONZE_CONTAINER_NAME)


def get_blob_client():
    return _get_container_client(SILVER_CONTAINER_NAME).get_blob_client(TABLA_RESERVAS_BLOB)


def leer_checkpoint() -> datetime | None:
    """Fecha (UTC) del blob mas reciente procesado en la ultima corrida, o
    None si todavia no corrio nunca."""
    blob_client = _get_container_client(SILVER_CONTAINER_NAME).get_blob_client(CHECKPOINT_BLOB)
    try:
        contenido = blob_client.download_blob().readall()
    except ResourceNotFoundError:
        return None
    return datetime.fromisoformat(json.loads(contenido)["ultima_corrida"])


def escribir_checkpoint(momento: datetime) -> None:
    blob_client = _get_container_client(SILVER_CONTAINER_NAME).get_blob_client(CHECKPOINT_BLOB)
    contenido = json.dumps({"ultima_corrida": momento.isoformat()})
    blob_client.upload_blob(contenido, overwrite=True)


def leer_tabla(blob_client) -> pd.DataFrame:
    """Descarga la tabla de silver; si todavia no existe (primera corrida),
    devuelve el esquema vacio en vez de fallar."""
    try:
        contenido = blob_client.download_blob().readall()
    except ResourceNotFoundError:
        return dataframe_vacio()
    return pd.read_parquet(BytesIO(contenido))


def escribir_tabla(blob_client, df: pd.DataFrame) -> None:
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    blob_client.upload_blob(buffer, overwrite=True)


# --- Dispatcher: aplica el evento correcto segun su eventType -------------

def aplicar_evento(df: pd.DataFrame, evento: dict) -> pd.DataFrame | None:
    """Aplica un evento de espacios (Creada o Actualizada) al DataFrame.
    Devuelve None si el eventType no es reconocido."""
    data = evento.get("data", {})
    event_type = evento.get("metadata", {}).get("eventType", "")
    tipo_evento = event_type.split(".")[-1]

    if tipo_evento == "ReservaCreada":
        return upsert_creada(df, data)
    if tipo_evento == "ReservaActualizada":
        return aplicar_actualizada(df, data)

    logging.warning(f"eventType inesperado para espacios: {event_type!r}")
    return None


# --- Trigger: corre 1 vez por dia -----------------------------------------

@bp.timer_trigger(
    schedule="0 0 0 * * *",
    arg_name="timer",
    run_on_startup=False,
    use_monitor=False,
)
def espacios_bronze_a_silver(timer: func.TimerRequest):
    """Corre una vez por dia. Lee de bronze/Espacios Publicos y Cultura/
    solo los blobs subidos despues del checkpoint de la corrida anterior
    (blobs.last_modified, que pone el propio storage) y los aplica sobre la
    tabla de silver existente."""
    checkpoint = leer_checkpoint()
    logging.info(f"Corrida diaria de espacios bronze->silver. Checkpoint anterior: {checkpoint}")

    bronze_container = get_bronze_container_client()
    blobs_nuevos = [
        blob for blob in bronze_container.list_blobs(name_starts_with=PREFIJO_ESPACIOS)
        if checkpoint is None or blob.last_modified > checkpoint
    ]
    blobs_nuevos.sort(key=lambda blob: blob.last_modified)

    if not blobs_nuevos:
        logging.info("No hay reservas nuevas ni actualizadas desde la ultima corrida.")
        return

    blob_client = get_blob_client()
    df = leer_tabla(blob_client)

    procesados = 0
    checkpoint_nuevo = checkpoint
    for blob in blobs_nuevos:
        contenido = bronze_container.download_blob(blob.name).readall()

        try:
            evento = json.loads(contenido)
        except json.JSONDecodeError:
            logging.warning(f"Blob no es JSON valido, se ignora: {blob.name}")
            continue

        df_actualizado = aplicar_evento(df, evento)
        if df_actualizado is not None:
            df = df_actualizado
            procesados += 1

        if checkpoint_nuevo is None or blob.last_modified > checkpoint_nuevo:
            checkpoint_nuevo = blob.last_modified

    escribir_tabla(blob_client, df)
    escribir_checkpoint(checkpoint_nuevo)
    logging.info(
        f"Procesados {procesados}/{len(blobs_nuevos)} eventos nuevos. "
        f"Tabla con {len(df)} filas. Nuevo checkpoint: {checkpoint_nuevo}"
    )
