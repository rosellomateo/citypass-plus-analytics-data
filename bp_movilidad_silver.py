"""Capa silver de movilidad urbana (Grupo 3: bicicletas publicas).

Timer trigger diario que lee los eventos crudos de bronze/Movilidad Urbana/
(ViajeIniciado y ViajeTerminado) y arma una tabla desnormalizada en
silver/Movilidad Urbana/viajes.parquet: una fila por viaje (join por `id`),
con los datos de inicio y fin del viaje juntos.

No todos los ViajeIniciado tienen su ViajeTerminado (viajes en curso a
proposito, ver README del Grupo 3) — esas filas quedan con las columnas de
fin en null y en_curso=True hasta que (si) llegue el evento correspondiente.

Procesa solo lo nuevo desde la ultima corrida (checkpoint mas abajo), mismo
patron que bp_reclamos_silver.py. Para reprocesar todo el historico usar
scripts/reprocesar_movilidad_silver.py.
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
PREFIJO_MOVILIDAD = "Movilidad Urbana/"
TABLA_VIAJES_BLOB = "Movilidad Urbana/viajes.parquet"
CHECKPOINT_BLOB = "Movilidad Urbana/_checkpoint.json"

COLUMNAS = [
    "viajeId",
    "correlationId",
    "estacionInicio",
    "barrioInicio",
    "latitudInicio",
    "longitudInicio",
    "horaInicio",
    "estacionFin",
    "latitudFin",
    "longitudFin",
    "horaFinalizacion",
    "en_curso",
    "duracion_minutos",
]


# --- Helpers para armar/actualizar una fila de la tabla -------------------

def _iso_a_datetime(iso_str):
    """horaInicio/horaFinalizacion vienen en ISO-8601; las pasamos a
    Timestamp UTC (pd.to_datetime tolera con/sin offset y el sufijo 'Z')."""
    if iso_str is None:
        return None
    return pd.to_datetime(iso_str, utc=True)


def dataframe_vacio() -> pd.DataFrame:
    """Tabla vacia con el esquema completo ya tipado, para arrancar desde
    cero o cuando todavia no existe el parquet en silver."""
    df = pd.DataFrame(columns=COLUMNAS)
    for col in ("horaInicio", "horaFinalizacion"):
        df[col] = pd.to_datetime(df[col], utc=True)
    df["en_curso"] = df["en_curso"].astype(bool)
    return df


def _fila_nueva(viaje_id: str) -> dict:
    """Fila placeholder para un viajeId que todavia no tiene datos (se usa
    tanto para un ViajeIniciado nuevo como para un ViajeTerminado que llega
    antes que su ViajeIniciado)."""
    fila = {col: None for col in COLUMNAS}
    fila["viajeId"] = viaje_id
    fila["en_curso"] = True
    return fila


def _actualizar_derivados(df: pd.DataFrame, idx) -> None:
    """en_curso = todavia no llego el ViajeTerminado. duracion_minutos solo
    se calcula si ya tenemos las dos puntas (inicio y fin) del viaje."""
    hora_inicio = df.at[idx, "horaInicio"]
    hora_fin = df.at[idx, "horaFinalizacion"]

    df.at[idx, "en_curso"] = pd.isna(hora_fin)

    if pd.isna(hora_inicio) or pd.isna(hora_fin):
        df.at[idx, "duracion_minutos"] = None
    else:
        df.at[idx, "duracion_minutos"] = (hora_fin - hora_inicio).total_seconds() / 60


# --- Aplicar eventos (Iniciado / Terminado) sobre el DataFrame ------------

def upsert_iniciado(df: pd.DataFrame, data: dict) -> pd.DataFrame:
    """Alta o completado de la fila del viaje a partir de un ViajeIniciado.
    Si el viaje ya tiene horaInicio cargada, es un ViajeIniciado duplicado
    (reentrega del evento): se ignora en vez de reprocesarlo."""
    viaje_id = data.get("id")

    if viaje_id in df["viajeId"].values:
        idx = df.index[df["viajeId"] == viaje_id][0]
        if pd.notna(df.at[idx, "horaInicio"]):
            logging.info(f"ViajeIniciado duplicado para {viaje_id}, se ignora.")
            return df
    else:
        df = pd.concat([df, pd.DataFrame([_fila_nueva(viaje_id)])], ignore_index=True)

    idx = df.index[df["viajeId"] == viaje_id][0]

    df.at[idx, "estacionInicio"] = data.get("estacion")
    df.at[idx, "barrioInicio"] = data.get("barrioInicio")
    df.at[idx, "latitudInicio"] = data.get("latitud")
    df.at[idx, "longitudInicio"] = data.get("longitud")
    df.at[idx, "horaInicio"] = _iso_a_datetime(data.get("horaInicio"))
    if data.get("correlationId") is not None:
        df.at[idx, "correlationId"] = data.get("correlationId")

    _actualizar_derivados(df, idx)
    return df


def aplicar_terminado(df: pd.DataFrame, data: dict) -> pd.DataFrame:
    """Completa la fila del viaje con los datos de un ViajeTerminado
    (creando una fila placeholder si el ViajeIniciado todavia no llego).
    Si el viaje ya tiene horaFinalizacion cargada, es un ViajeTerminado
    duplicado (reentrega del evento): se ignora en vez de reprocesarlo."""
    viaje_id = data.get("id")

    if viaje_id in df["viajeId"].values:
        idx = df.index[df["viajeId"] == viaje_id][0]
        if pd.notna(df.at[idx, "horaFinalizacion"]):
            logging.info(f"ViajeTerminado duplicado para {viaje_id}, se ignora.")
            return df
    else:
        df = pd.concat([df, pd.DataFrame([_fila_nueva(viaje_id)])], ignore_index=True)

    idx = df.index[df["viajeId"] == viaje_id][0]

    df.at[idx, "estacionFin"] = data.get("estacion")
    df.at[idx, "latitudFin"] = data.get("latitud")
    df.at[idx, "longitudFin"] = data.get("longitud")
    df.at[idx, "horaFinalizacion"] = _iso_a_datetime(data.get("horaFinalizacion"))
    if data.get("correlationId") is not None:
        df.at[idx, "correlationId"] = data.get("correlationId")

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
    return _get_container_client(SILVER_CONTAINER_NAME).get_blob_client(TABLA_VIAJES_BLOB)


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
    """Aplica un evento de movilidad (Iniciado o Terminado) al DataFrame.
    Devuelve None si el eventType no es reconocido."""
    data = evento.get("data", {})
    event_type = evento.get("metadata", {}).get("eventType", "")
    tipo_evento = event_type.split(".")[-1]

    if tipo_evento == "ViajeIniciado":
        return upsert_iniciado(df, data)
    if tipo_evento == "ViajeTerminado":
        return aplicar_terminado(df, data)

    logging.warning(f"eventType inesperado para movilidad: {event_type!r}")
    return None


# --- Trigger: corre 1 vez por dia -----------------------------------------

@bp.timer_trigger(
    schedule="0 0 0 * * *",
    arg_name="timer",
    run_on_startup=False,
    use_monitor=False,
)
def movilidad_bronze_a_silver(timer: func.TimerRequest):
    """Corre una vez por dia. Lee de bronze/Movilidad Urbana/ solo los blobs
    subidos despues del checkpoint de la corrida anterior (blobs.last_modified,
    que pone el propio storage) y los aplica sobre la tabla de silver existente."""
    checkpoint = leer_checkpoint()
    logging.info(f"Corrida diaria de movilidad bronze->silver. Checkpoint anterior: {checkpoint}")

    bronze_container = get_bronze_container_client()
    blobs_nuevos = [
        blob for blob in bronze_container.list_blobs(name_starts_with=PREFIJO_MOVILIDAD)
        if checkpoint is None or blob.last_modified > checkpoint
    ]
    blobs_nuevos.sort(key=lambda blob: blob.last_modified)

    if not blobs_nuevos:
        logging.info("No hay viajes nuevos ni terminados desde la ultima corrida.")
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
