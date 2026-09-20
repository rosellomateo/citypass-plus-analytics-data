"""Capa silver de gestion de residuos inteligente.

Timer trigger diario que lee los eventos crudos de bronze/Gestion de
Residuos Inteligente/ (AlertaGenerada y RecoleccionCompletada) y arma una
tabla en silver/Gestion de Residuos Inteligente/alertas.parquet: una fila
por alerta, enriquecida con los datos de la recoleccion que la resolvio
(si existe).

A diferencia de movilidad (mismo id en ambos eventos), aca AlertaGenerada y
RecoleccionCompletada tienen ids distintos (alertaId / recoleccionId) y la
relacion es opcional: una RecoleccionCompletada con alertaId=null viene de
un recorrido programado, no de una alerta, y por lo tanto no tiene fila en
esta tabla (se loguea, no se pierde en silencio, pero no se representa aca).

Procesa solo lo nuevo desde la ultima corrida (checkpoint mas abajo), mismo
patron que bp_reclamos_silver.py / bp_movilidad_silver.py. Para reprocesar
todo el historico usar scripts/reprocesar_residuos_silver.py.
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
PREFIJO_RESIDUOS = "Gestion de Residuos Inteligente/"
TABLA_ALERTAS_BLOB = "Gestion de Residuos Inteligente/alertas.parquet"
CHECKPOINT_BLOB = "Gestion de Residuos Inteligente/_checkpoint.json"

COLUMNAS_BASE = [
    "alertaId",
    "contenedorId",
    "tipoAlerta",
    "nivelLlenado",
    "zona",
    "prioridad",
    "correlationId",
    "fechaAlerta",
]

COLUMNAS = [
    *COLUMNAS_BASE,
    "recoleccionId",
    "camionId",
    "fechaRecoleccion",
    "resuelta",
    "tiempo_resolucion_minutos",
]


# --- Helpers para armar/actualizar una fila de la tabla -------------------

def _iso_a_datetime(iso_str):
    """occurredAt viene en ISO-8601; lo pasamos a Timestamp UTC
    (pd.to_datetime tolera con/sin offset y el sufijo 'Z')."""
    if iso_str is None:
        return None
    return pd.to_datetime(iso_str, utc=True)


def dataframe_vacio() -> pd.DataFrame:
    """Tabla vacia con el esquema completo ya tipado, para arrancar desde
    cero o cuando todavia no existe el parquet en silver."""
    df = pd.DataFrame(columns=COLUMNAS)
    for col in ("fechaAlerta", "fechaRecoleccion"):
        df[col] = pd.to_datetime(df[col], utc=True)
    df["resuelta"] = df["resuelta"].astype(bool)
    return df


def _fila_nueva(alerta_id: str) -> dict:
    """Fila placeholder para un alertaId que todavia no tiene datos (se usa
    tanto para una AlertaGenerada nueva como para una RecoleccionCompletada
    que llega antes que su AlertaGenerada)."""
    fila = {col: None for col in COLUMNAS}
    fila["alertaId"] = alerta_id
    fila["resuelta"] = False
    return fila


def _actualizar_derivados(df: pd.DataFrame, idx) -> None:
    """resuelta = ya llego la RecoleccionCompletada que cierra esta alerta.
    tiempo_resolucion_minutos solo se calcula si ya tenemos las dos puntas."""
    fecha_alerta = df.at[idx, "fechaAlerta"]
    fecha_recoleccion = df.at[idx, "fechaRecoleccion"]

    df.at[idx, "resuelta"] = pd.notna(fecha_recoleccion)

    if pd.isna(fecha_alerta) or pd.isna(fecha_recoleccion):
        df.at[idx, "tiempo_resolucion_minutos"] = None
    else:
        df.at[idx, "tiempo_resolucion_minutos"] = (fecha_recoleccion - fecha_alerta).total_seconds() / 60


# --- Aplicar eventos (Alerta / Recoleccion) sobre el DataFrame ------------

def upsert_alerta(df: pd.DataFrame, data: dict) -> pd.DataFrame:
    """Alta o completado de la fila a partir de una AlertaGenerada. Si la
    alerta ya tiene fechaAlerta cargada, es un evento duplicado: se ignora
    en vez de reprocesarlo."""
    alerta_id = data.get("alertaId")

    if alerta_id in df["alertaId"].values:
        idx = df.index[df["alertaId"] == alerta_id][0]
        if pd.notna(df.at[idx, "fechaAlerta"]):
            logging.info(f"AlertaGenerada duplicada para {alerta_id}, se ignora.")
            return df
    else:
        df = pd.concat([df, pd.DataFrame([_fila_nueva(alerta_id)])], ignore_index=True)

    idx = df.index[df["alertaId"] == alerta_id][0]

    df.at[idx, "contenedorId"] = data.get("contenedorId")
    df.at[idx, "tipoAlerta"] = data.get("tipoAlerta")
    df.at[idx, "nivelLlenado"] = data.get("nivelLlenado")
    df.at[idx, "zona"] = data.get("zona")
    df.at[idx, "prioridad"] = data.get("prioridad")
    df.at[idx, "correlationId"] = data.get("correlationId")
    df.at[idx, "fechaAlerta"] = _iso_a_datetime(data.get("occurredAt"))

    _actualizar_derivados(df, idx)
    return df


def aplicar_recoleccion(df: pd.DataFrame, data: dict) -> pd.DataFrame | None:
    """Completa la fila de la alerta con los datos de la RecoleccionCompletada
    que la resolvio (creando una fila placeholder si la AlertaGenerada
    todavia no llego). Si la recoleccion no trae alertaId (recorrido
    programado) o si esa alerta ya tenia una recoleccion cargada
    (duplicado), no hay nada que actualizar y se devuelve None."""
    alerta_id = data.get("alertaId")
    if alerta_id is None:
        logging.info(
            f"RecoleccionCompletada {data.get('recoleccionId')} sin alertaId "
            "(recorrido programado): no se refleja en la tabla de alertas."
        )
        return None

    if alerta_id in df["alertaId"].values:
        idx = df.index[df["alertaId"] == alerta_id][0]
        if pd.notna(df.at[idx, "recoleccionId"]):
            logging.info(f"RecoleccionCompletada duplicada para alerta {alerta_id}, se ignora.")
            return None
    else:
        df = pd.concat([df, pd.DataFrame([_fila_nueva(alerta_id)])], ignore_index=True)

    idx = df.index[df["alertaId"] == alerta_id][0]

    df.at[idx, "recoleccionId"] = data.get("recoleccionId")
    df.at[idx, "camionId"] = data.get("camionId")
    df.at[idx, "fechaRecoleccion"] = _iso_a_datetime(data.get("occurredAt"))

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
    return _get_container_client(SILVER_CONTAINER_NAME).get_blob_client(TABLA_ALERTAS_BLOB)


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
    """Aplica un evento de residuos (AlertaGenerada o RecoleccionCompletada)
    al DataFrame. Devuelve None si el eventType no es reconocido, o si el
    evento es valido pero no corresponde reflejarlo en esta tabla (ver
    aplicar_recoleccion)."""
    data = evento.get("data", {})
    event_type = evento.get("metadata", {}).get("eventType", "")
    tipo_evento = event_type.split(".")[-1]

    if tipo_evento == "AlertaGenerada":
        return upsert_alerta(df, data)
    if tipo_evento == "RecoleccionCompletada":
        return aplicar_recoleccion(df, data)

    logging.warning(f"eventType inesperado para residuos: {event_type!r}")
    return None


# --- Trigger: corre 1 vez por dia -----------------------------------------

@bp.timer_trigger(
    schedule="0 0 0 * * *",
    arg_name="timer",
    run_on_startup=False,
    use_monitor=False,
)
def residuos_bronze_a_silver(timer: func.TimerRequest):
    """Corre una vez por dia. Lee de bronze/Gestion de Residuos Inteligente/
    solo los blobs subidos despues del checkpoint de la corrida anterior
    (blobs.last_modified, que pone el propio storage) y los aplica sobre la
    tabla de silver existente."""
    checkpoint = leer_checkpoint()
    logging.info(f"Corrida diaria de residuos bronze->silver. Checkpoint anterior: {checkpoint}")

    bronze_container = get_bronze_container_client()
    blobs_nuevos = [
        blob for blob in bronze_container.list_blobs(name_starts_with=PREFIJO_RESIDUOS)
        if checkpoint is None or blob.last_modified > checkpoint
    ]
    blobs_nuevos.sort(key=lambda blob: blob.last_modified)

    if not blobs_nuevos:
        logging.info("No hay alertas ni recolecciones nuevas desde la ultima corrida.")
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
