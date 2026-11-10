"""Capa silver de autenticacion (LDAP / Login Federado, Grupo 9).

Timer trigger diario que lee los eventos crudos de bronze/LDAP/
(LoginExitoso, TokenRenovado, SesionCerrada) y arma una tabla plana en
silver/LDAP/eventos_autenticacion.parquet: una fila por evento, no
por entidad -- a diferencia de reclamos/emergencias/movilidad, aca no hay un
ciclo de vida que reconstruir: los 3 eventos son hechos independientes que
se guardan tal cual.

Los 3 comparten la misma forma (authEventId, actorSub, occurredAt + 6 campos
opcionales: username, department, clientId, chainId, ipAddress, userAgent),
asi que alcanza con una sola tabla; la columna event_type distingue cual es
cual (estructura sugerida por el propio Eventos-login.md, seccion
"Recomendaciones para el consumidor").

Deduplicacion por auth_event_id (el authEventId que manda el productor,
pensado justamente para esto segun Eventos-login.md).

Procesa solo lo nuevo desde la ultima corrida (checkpoint mas abajo). Para
reprocesar todo el historico usar scripts/reprocesar_autenticacion_silver.py.
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
PREFIJO_AUTENTICACION = "LDAP/"
TABLA_AUTENTICACION_BLOB = "LDAP/eventos_autenticacion.parquet"
CHECKPOINT_BLOB = "LDAP/_checkpoint.json"

EVENT_TYPES_VALIDOS = ("LoginExitoso", "TokenRenovado", "SesionCerrada")

COLUMNAS = [
    "event_type",
    "auth_event_id",
    "event_time_utc",
    "actor_sub",
    "username",
    "department",
    "client_id",
    "chain_id",
    "ip_address",
    "user_agent",
    "ingested_at",
]


# --- Helpers para armar una fila ------------------------------------------

def _epoch_millis_a_datetime(epoch_millis):
    """occurredAt viene en epoch milliseconds UTC (ver Eventos-login.md,
    'Tratamiento del tiempo'). Si no es un numero valido, se guarda como
    fecha ausente en vez de romper el reproceso entero."""
    if epoch_millis is None:
        return None
    try:
        return pd.to_datetime(epoch_millis, unit="ms", utc=True)
    except (ValueError, TypeError):
        logging.warning(f"occurredAt no es un epoch valido: {epoch_millis!r}")
        return None


def dataframe_vacio() -> pd.DataFrame:
    """Tabla vacia con el esquema completo ya tipado, para arrancar desde
    cero o cuando todavia no existe el parquet en silver."""
    df = pd.DataFrame(columns=COLUMNAS)
    for col in ("event_time_utc", "ingested_at"):
        df[col] = pd.to_datetime(df[col], utc=True)
    return df


def aplicar_evento(df: pd.DataFrame, evento: dict, ingested_at: datetime) -> pd.DataFrame | None:
    """Agrega una fila por el evento (LoginExitoso, TokenRenovado o
    SesionCerrada -- los 3 se guardan igual, solo cambia event_type).
    Devuelve None si el eventType no es ninguno de los 3 esperados. Si el
    auth_event_id ya esta en la tabla, es un evento duplicado: se ignora y
    se devuelve la tabla sin cambios (no None, para que se cuente como
    procesado y avance el checkpoint)."""
    data = evento.get("data", {})
    event_type = evento.get("metadata", {}).get("eventType", "")
    tipo_evento = event_type.split(".")[-1]

    if tipo_evento not in EVENT_TYPES_VALIDOS:
        logging.warning(f"eventType inesperado para autenticacion: {event_type!r}")
        return None

    auth_event_id = data.get("authEventId")
    if auth_event_id in df["auth_event_id"].values:
        logging.info(f"Evento de autenticacion duplicado, se ignora: {auth_event_id}")
        return df

    fila = {
        "event_type": event_type,
        "auth_event_id": auth_event_id,
        "event_time_utc": _epoch_millis_a_datetime(data.get("occurredAt")),
        "actor_sub": data.get("actorSub"),
        "username": data.get("username"),
        "department": data.get("department"),
        "client_id": data.get("clientId"),
        "chain_id": data.get("chainId"),
        "ip_address": data.get("ipAddress"),
        "user_agent": data.get("userAgent"),
        "ingested_at": ingested_at,
    }
    return pd.concat([df, pd.DataFrame([fila])], ignore_index=True)


# --- Acceso a storage (bronze/silver) -------------------------------------

def _get_container_client(nombre_container: str):
    connection_string = os.environ["STORAGE_CONNECTION_STRING"]
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    return blob_service_client.get_container_client(nombre_container)


def get_bronze_container_client():
    return _get_container_client(BRONZE_CONTAINER_NAME)


def get_blob_client():
    return _get_container_client(SILVER_CONTAINER_NAME).get_blob_client(TABLA_AUTENTICACION_BLOB)


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


# --- Trigger: corre 1 vez por dia -----------------------------------------

@bp.timer_trigger(
    schedule="0 0 0 * * *",
    arg_name="timer",
    run_on_startup=False,
    use_monitor=False,
)
def autenticacion_bronze_a_silver(timer: func.TimerRequest):
    """Corre una vez por dia. Lee de bronze/LDAP/ solo los blobs
    subidos despues del checkpoint de la corrida anterior (blob.last_modified,
    que pone el propio storage y que usamos como ingested_at de cada fila) y
    los agrega a la tabla de silver existente."""
    checkpoint = leer_checkpoint()
    logging.info(f"Corrida diaria de autenticacion bronze->silver. Checkpoint anterior: {checkpoint}")

    bronze_container = get_bronze_container_client()
    blobs_nuevos = [
        blob for blob in bronze_container.list_blobs(name_starts_with=PREFIJO_AUTENTICACION)
        if checkpoint is None or blob.last_modified > checkpoint
    ]
    blobs_nuevos.sort(key=lambda blob: blob.last_modified)

    if not blobs_nuevos:
        logging.info("No hay eventos de autenticacion nuevos desde la ultima corrida.")
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

        df_actualizado = aplicar_evento(df, evento, ingested_at=blob.last_modified)
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
