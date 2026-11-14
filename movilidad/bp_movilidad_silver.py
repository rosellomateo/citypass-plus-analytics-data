"""Capa silver de movilidad urbana (Grupo 3).

Timer trigger diario que lee los eventos crudos de bronze/Movilidad Urbana/
y arma DOS tablas separadas en silver/Movilidad Urbana/:

  - viajes.parquet: una fila por viajeId, a partir de ViajeFinalizado. A
    diferencia del resto de los dominios, este evento ya trae el viaje
    completo (inicio + fin) en un solo mensaje, asi que no hace falta join
    ni estado "en curso": la fila queda completa apenas llega el evento.

  - estaciones_afectadas.parquet: una fila por afectacionId, a partir de
    EstacionUrbanaAfectada (alta) y EstacionUrbanaRestaurada (cierra la
    afectacion, join por afectacionId).

NOTA (14/nov): el modelo anterior de este modulo era ViajeIniciado/
ViajeTerminado, pero esos eventos nunca existieron en el bus real
(verificado contra /event-types del schema registry). Movilidad publica
ViajeFinalizado en su lugar -- ver CONTRATO_MOVILIDAD.md #2.3 (evento nuevo,
propuesto por nosotros).

Procesa solo lo nuevo desde la ultima corrida (checkpoint propio por tabla),
mismo patron que bp_reclamos_silver.py. Para reprocesar todo el historico
usar scripts/reprocesar_movilidad_silver.py.
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


# --- Acceso a storage (bronze/silver) -------------------------------------

def _get_container_client(nombre_container: str):
    connection_string = os.environ["STORAGE_CONNECTION_STRING"]
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    return blob_service_client.get_container_client(nombre_container)


def get_bronze_container_client():
    return _get_container_client(BRONZE_CONTAINER_NAME)


def escribir_tabla(blob_client, df: pd.DataFrame) -> None:
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    blob_client.upload_blob(buffer, overwrite=True)


def _iso_a_datetime(iso_str):
    """horaInicio/horaFinalizacion/afectacionDesde/etc. vienen en ISO-8601;
    las pasamos a Timestamp UTC (pd.to_datetime tolera con/sin offset y el
    sufijo 'Z')."""
    if iso_str is None:
        return None
    return pd.to_datetime(iso_str, utc=True)


# ===========================================================================
# Viajes (ViajeFinalizado)
# ===========================================================================

TABLA_VIAJES_BLOB = "Movilidad Urbana/viajes.parquet"
CHECKPOINT_BLOB = "Movilidad Urbana/_checkpoint.json"

COLUMNAS = [
    "viajeId",
    "correlationId",
    "estacionInicio",
    "latitudInicio",
    "longitudInicio",
    "horaInicio",
    "estacionFin",
    "latitudFin",
    "longitudFin",
    "horaFinalizacion",
    "causaId",
    "causaTipo",
    "createdAt",
    "duracion_minutos",
]


def dataframe_vacio() -> pd.DataFrame:
    """Tabla vacia con el esquema completo ya tipado, para arrancar desde
    cero o cuando todavia no existe el parquet en silver."""
    df = pd.DataFrame(columns=COLUMNAS)
    for col in ("horaInicio", "horaFinalizacion", "createdAt"):
        df[col] = pd.to_datetime(df[col], utc=True)
    return df


def _fila_nueva_viaje(viaje_id: str) -> dict:
    fila = {col: None for col in COLUMNAS}
    fila["viajeId"] = viaje_id
    return fila


def upsert_viaje_finalizado(df: pd.DataFrame, data: dict) -> pd.DataFrame:
    """Alta de la fila a partir de un ViajeFinalizado. El evento trae el
    viaje completo (inicio + fin) en un solo mensaje, asi que la fila queda
    completa de una: no hay estado intermedio. Si el viaje ya tiene
    horaFinalizacion cargada, es un evento duplicado (reentrega): se
    ignora."""
    viaje_id = data.get("viajeId")

    if viaje_id in df["viajeId"].values:
        idx = df.index[df["viajeId"] == viaje_id][0]
        if pd.notna(df.at[idx, "horaFinalizacion"]):
            logging.info(f"ViajeFinalizado duplicado para {viaje_id}, se ignora.")
            return df
    else:
        df = pd.concat([df, pd.DataFrame([_fila_nueva_viaje(viaje_id)])], ignore_index=True)

    idx = df.index[df["viajeId"] == viaje_id][0]

    for col in COLUMNAS:
        if col not in ("viajeId", "horaInicio", "horaFinalizacion", "createdAt", "duracion_minutos"):
            df.at[idx, col] = data.get(col)

    df.at[idx, "horaInicio"] = _iso_a_datetime(data.get("horaInicio"))
    df.at[idx, "horaFinalizacion"] = _iso_a_datetime(data.get("horaFinalizacion"))
    df.at[idx, "createdAt"] = _iso_a_datetime(data.get("createdAt"))

    hora_inicio = df.at[idx, "horaInicio"]
    hora_fin = df.at[idx, "horaFinalizacion"]
    if pd.isna(hora_inicio) or pd.isna(hora_fin):
        df.at[idx, "duracion_minutos"] = None
    else:
        df.at[idx, "duracion_minutos"] = (hora_fin - hora_inicio).total_seconds() / 60

    return df


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


def aplicar_evento(df: pd.DataFrame, evento: dict) -> pd.DataFrame | None:
    """Aplica un evento de viajes (ViajeFinalizado) al DataFrame. Devuelve
    None si el eventType no es reconocido."""
    data = evento.get("data", {})
    event_type = evento.get("metadata", {}).get("eventType", "")
    tipo_evento = event_type.split(".")[-1]

    if tipo_evento == "ViajeFinalizado":
        return upsert_viaje_finalizado(df, data)

    return None


@bp.timer_trigger(
    schedule="0 0 0 * * *",
    arg_name="timer",
    run_on_startup=False,
    use_monitor=False,
)
def movilidad_bronze_a_silver(timer: func.TimerRequest):
    """Corre una vez por dia. Lee de bronze/Movilidad Urbana/ solo los blobs
    subidos despues del checkpoint de la corrida anterior (blobs.last_modified,
    que pone el propio storage) y aplica los que sean ViajeFinalizado; el
    resto (afectaciones de estacion) los ignora en silencio, los procesa el
    otro trigger."""
    checkpoint = leer_checkpoint()
    logging.info(f"Corrida diaria de movilidad (viajes) bronze->silver. Checkpoint anterior: {checkpoint}")

    bronze_container = get_bronze_container_client()
    blobs_nuevos = [
        blob for blob in bronze_container.list_blobs(name_starts_with=PREFIJO_MOVILIDAD)
        if checkpoint is None or blob.last_modified > checkpoint
    ]
    blobs_nuevos.sort(key=lambda blob: blob.last_modified)

    if not blobs_nuevos:
        logging.info("No hay viajes nuevos desde la ultima corrida.")
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
        f"Procesados {procesados}/{len(blobs_nuevos)} viajes nuevos. "
        f"Tabla con {len(df)} filas. Nuevo checkpoint: {checkpoint_nuevo}"
    )


# ===========================================================================
# Estaciones afectadas (EstacionUrbanaAfectada / EstacionUrbanaRestaurada)
# ===========================================================================

TABLA_ESTACIONES_BLOB = "Movilidad Urbana/estaciones_afectadas.parquet"
CHECKPOINT_ESTACIONES_BLOB = "Movilidad Urbana/_checkpoint_estaciones.json"

COLUMNAS_BASE_ESTACION = [
    "afectacionId",
    "estacionId",
    "latitud",
    "longitud",
    "tipoAfectacion",
    "afectacionDesde",
    "afectacionHastaEstimada",
    "causaId",
    "causaTipo",
    "moduloOrigen",
    "correlationId",
    "occurredAt",
]

COLUMNAS_ESTACION = [
    *COLUMNAS_BASE_ESTACION,
    "restaurada",
    "restauradaAt",
]


def dataframe_vacio_estaciones() -> pd.DataFrame:
    """Tabla vacia con el esquema completo ya tipado, para arrancar desde
    cero o cuando todavia no existe el parquet en silver."""
    df = pd.DataFrame(columns=COLUMNAS_ESTACION)
    for col in ("afectacionDesde", "afectacionHastaEstimada", "occurredAt", "restauradaAt"):
        df[col] = pd.to_datetime(df[col], utc=True)
    df["restaurada"] = df["restaurada"].astype(bool)
    return df


def _fila_nueva_estacion(afectacion_id: str) -> dict:
    fila = {col: None for col in COLUMNAS_ESTACION}
    fila["afectacionId"] = afectacion_id
    fila["restaurada"] = False
    return fila


def upsert_estacion_afectada(df: pd.DataFrame, data: dict) -> pd.DataFrame:
    """Alta de la fila a partir de una EstacionUrbanaAfectada (creando una
    fila placeholder si una EstacionUrbanaRestaurada ya llego antes, fuera
    de orden). Si la afectacion ya tiene tipoAfectacion cargado, es un
    evento duplicado (reentrega): se ignora."""
    afectacion_id = data.get("afectacionId")

    if afectacion_id in df["afectacionId"].values:
        idx = df.index[df["afectacionId"] == afectacion_id][0]
        if pd.notna(df.at[idx, "tipoAfectacion"]):
            logging.info(f"EstacionUrbanaAfectada duplicada para {afectacion_id}, se ignora.")
            return df
    else:
        df = pd.concat([df, pd.DataFrame([_fila_nueva_estacion(afectacion_id)])], ignore_index=True)

    idx = df.index[df["afectacionId"] == afectacion_id][0]

    for col in COLUMNAS_BASE_ESTACION:
        if col not in ("afectacionId", "afectacionDesde", "afectacionHastaEstimada", "occurredAt"):
            df.at[idx, col] = data.get(col)

    df.at[idx, "afectacionDesde"] = _iso_a_datetime(data.get("afectacionDesde"))
    df.at[idx, "afectacionHastaEstimada"] = _iso_a_datetime(data.get("afectacionHastaEstimada"))
    df.at[idx, "occurredAt"] = _iso_a_datetime(data.get("occurredAt"))

    return df


def aplicar_estacion_restaurada(df: pd.DataFrame, data: dict) -> pd.DataFrame:
    """Aplica una EstacionUrbanaRestaurada (creando una fila placeholder si
    la EstacionUrbanaAfectada todavia no llego). Si la afectacion ya esta
    marcada restaurada, es un evento duplicado (reentrega): se ignora."""
    afectacion_id = data.get("afectacionId")

    if afectacion_id in df["afectacionId"].values:
        idx = df.index[df["afectacionId"] == afectacion_id][0]
        if df.at[idx, "restaurada"]:
            logging.info(f"EstacionUrbanaRestaurada duplicada para {afectacion_id}, se ignora.")
            return df
    else:
        df = pd.concat([df, pd.DataFrame([_fila_nueva_estacion(afectacion_id)])], ignore_index=True)

    idx = df.index[df["afectacionId"] == afectacion_id][0]

    if pd.isna(df.at[idx, "estacionId"]):
        df.at[idx, "estacionId"] = data.get("estacionId")

    df.at[idx, "restaurada"] = True
    df.at[idx, "restauradaAt"] = _iso_a_datetime(data.get("restauradaAt"))

    return df


def get_blob_client_estaciones():
    return _get_container_client(SILVER_CONTAINER_NAME).get_blob_client(TABLA_ESTACIONES_BLOB)


def leer_checkpoint_estaciones() -> datetime | None:
    blob_client = _get_container_client(SILVER_CONTAINER_NAME).get_blob_client(CHECKPOINT_ESTACIONES_BLOB)
    try:
        contenido = blob_client.download_blob().readall()
    except ResourceNotFoundError:
        return None
    return datetime.fromisoformat(json.loads(contenido)["ultima_corrida"])


def escribir_checkpoint_estaciones(momento: datetime) -> None:
    blob_client = _get_container_client(SILVER_CONTAINER_NAME).get_blob_client(CHECKPOINT_ESTACIONES_BLOB)
    contenido = json.dumps({"ultima_corrida": momento.isoformat()})
    blob_client.upload_blob(contenido, overwrite=True)


def leer_tabla_estaciones(blob_client) -> pd.DataFrame:
    try:
        contenido = blob_client.download_blob().readall()
    except ResourceNotFoundError:
        return dataframe_vacio_estaciones()
    return pd.read_parquet(BytesIO(contenido))


def aplicar_evento_estacion(df: pd.DataFrame, evento: dict) -> pd.DataFrame | None:
    """Aplica un evento de estaciones afectadas (Afectada o Restaurada) al
    DataFrame. Devuelve None si el eventType no es reconocido."""
    data = evento.get("data", {})
    event_type = evento.get("metadata", {}).get("eventType", "")
    tipo_evento = event_type.split(".")[-1]

    if tipo_evento == "EstacionUrbanaAfectada":
        return upsert_estacion_afectada(df, data)
    if tipo_evento == "EstacionUrbanaRestaurada":
        return aplicar_estacion_restaurada(df, data)

    return None


@bp.timer_trigger(
    schedule="0 0 0 * * *",
    arg_name="timer",
    run_on_startup=False,
    use_monitor=False,
)
def movilidad_estaciones_bronze_a_silver(timer: func.TimerRequest):
    """Corre una vez por dia. Lee de bronze/Movilidad Urbana/ solo los blobs
    subidos despues del checkpoint de estaciones (propio, independiente del
    de viajes) y aplica los que sean EstacionUrbanaAfectada/Restaurada."""
    checkpoint = leer_checkpoint_estaciones()
    logging.info(f"Corrida diaria de movilidad (estaciones) bronze->silver. Checkpoint anterior: {checkpoint}")

    bronze_container = get_bronze_container_client()
    blobs_nuevos = [
        blob for blob in bronze_container.list_blobs(name_starts_with=PREFIJO_MOVILIDAD)
        if checkpoint is None or blob.last_modified > checkpoint
    ]
    blobs_nuevos.sort(key=lambda blob: blob.last_modified)

    if not blobs_nuevos:
        logging.info("No hay afectaciones de estacion nuevas desde la ultima corrida.")
        return

    blob_client = get_blob_client_estaciones()
    df = leer_tabla_estaciones(blob_client)

    procesados = 0
    checkpoint_nuevo = checkpoint
    for blob in blobs_nuevos:
        contenido = bronze_container.download_blob(blob.name).readall()

        try:
            evento = json.loads(contenido)
        except json.JSONDecodeError:
            logging.warning(f"Blob no es JSON valido, se ignora: {blob.name}")
            continue

        df_actualizado = aplicar_evento_estacion(df, evento)
        if df_actualizado is not None:
            df = df_actualizado
            procesados += 1

        if checkpoint_nuevo is None or blob.last_modified > checkpoint_nuevo:
            checkpoint_nuevo = blob.last_modified

    escribir_tabla(blob_client, df)
    escribir_checkpoint_estaciones(checkpoint_nuevo)
    logging.info(
        f"Procesados {procesados}/{len(blobs_nuevos)} eventos de estacion nuevos. "
        f"Tabla con {len(df)} filas. Nuevo checkpoint: {checkpoint_nuevo}"
    )
