"""Capa silver de reclamos.

Timer trigger diario que lee los eventos crudos de bronze/Reclamos/
(ReclamoCreado y ReclamoActualizado) y arma una tabla desnormalizada en
silver/Reclamos/reclamos.parquet: una fila por reclamo, con el historial de
fechas por estado, estado_actual/estado_anterior y unos booleanos derivados.

Procesa solo lo nuevo desde la ultima corrida (ver checkpoint mas abajo), no
relee bronze entero cada vez. Para reprocesar todo el historico (por ejemplo
si cambia el esquema de la tabla) usar scripts/reprocesar_reclamos_silver.py.
"""
import azure.functions as func
import logging
import json
import os
from datetime import datetime, timezone
from io import BytesIO

import pandas as pd
from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob import BlobServiceClient

bp = func.Blueprint()

# --- Configuracion: containers, paths de blobs ---------------------------

BRONZE_CONTAINER_NAME = "bronze"
SILVER_CONTAINER_NAME = "silver"
PREFIJO_RECLAMOS = "Reclamos/"
TABLA_RECLAMOS_BLOB = "Reclamos/reclamos.parquet"
CHECKPOINT_BLOB = "Reclamos/_checkpoint.json"

# Estados de la maquina de estados de reclamos (ver README del Grupo 4).
ESTADOS = [
    "RECIBIDO",
    "EN_REVISION",
    "ASIGNADO",
    "EN_PROCESO",
    "RESUELTO",
    "RECHAZADO",
    "CERRADO",
]

COLUMNA_FECHA_POR_ESTADO = {estado: f"fecha_{estado.lower()}" for estado in ESTADOS}
# Excepcion de nombre: RECIBIDO se guarda como fecha_creado, no fecha_recibido.
COLUMNA_FECHA_POR_ESTADO["RECIBIDO"] = "fecha_creado"
ESTADO_POR_COLUMNA_FECHA = {columna: estado for estado, columna in COLUMNA_FECHA_POR_ESTADO.items()}

COLUMNAS_BASE = [
    "reclamoId",
    "ciudadanoId",
    "canal",
    "titulo",
    "descripcion",
    "categoria",
    "prioridad",
    "origenClasificacion",
    "confianzaClasificacion",
    "direccion",
    "barrio",
    "latitud",
    "longitud",
    "correlationId",
]

COLUMNAS = [
    *COLUMNAS_BASE,
    *COLUMNA_FECHA_POR_ESTADO.values(),
    "estado_actual",
    "estado_anterior",
    "eventos_actualizado_aplicados",
    "resuelto",
    "rechazado",
    "cerrado",
]


# --- Helpers para armar/actualizar una fila de la tabla -------------------

def _epoch_ms_a_datetime(epoch_ms):
    """Timestamps de los eventos vienen en epoch millis; los pasamos a
    datetime UTC para poder compararlos y guardarlos como fecha en el
    parquet."""
    if epoch_ms is None:
        return None
    return datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc)


def dataframe_vacio() -> pd.DataFrame:
    """Tabla vacia con el esquema completo ya tipado (fechas UTC, booleanos),
    para arrancar desde cero o cuando todavia no existe el parquet en silver."""
    df = pd.DataFrame(columns=COLUMNAS)
    for col in COLUMNA_FECHA_POR_ESTADO.values():
        df[col] = pd.to_datetime(df[col], utc=True)
    for col in ("resuelto", "rechazado", "cerrado"):
        df[col] = df[col].astype(bool)
    return df


def _fila_nueva(reclamo_id: str) -> dict:
    """Fila placeholder para un reclamoId que todavia no tiene datos (se usa
    tanto para un ReclamoCreado nuevo como para un ReclamoActualizado que
    llega antes que su ReclamoCreado)."""
    fila = {col: None for col in COLUMNAS}
    fila["reclamoId"] = reclamo_id
    fila["resuelto"] = False
    fila["rechazado"] = False
    fila["cerrado"] = False
    fila["eventos_actualizado_aplicados"] = []
    return fila


def _actualizar_estado_actual(df: pd.DataFrame, idx) -> None:
    """estado_actual = el estado cuya fecha es la mas reciente de todas las
    cargadas en la fila. No depende del orden de llegada de los eventos,
    solo de las fechas que traen (igual que el resto de las columnas)."""
    fechas = {
        columna: df.at[idx, columna]
        for columna in COLUMNA_FECHA_POR_ESTADO.values()
        if pd.notna(df.at[idx, columna])
    }

    if not fechas:
        df.at[idx, "estado_actual"] = None
        return

    columna_mas_reciente = max(fechas, key=fechas.get)
    df.at[idx, "estado_actual"] = ESTADO_POR_COLUMNA_FECHA[columna_mas_reciente]


def _actualizar_derivados(df: pd.DataFrame, idx) -> None:
    """Recalcula, para una fila, todo lo que depende de las fechas de estado
    ya cargadas: los booleanos y estado_actual."""
    df.at[idx, "resuelto"] = pd.notna(df.at[idx, COLUMNA_FECHA_POR_ESTADO["RESUELTO"]])
    df.at[idx, "rechazado"] = pd.notna(df.at[idx, COLUMNA_FECHA_POR_ESTADO["RECHAZADO"]])
    df.at[idx, "cerrado"] = pd.notna(df.at[idx, COLUMNA_FECHA_POR_ESTADO["CERRADO"]])
    _actualizar_estado_actual(df, idx)


def _es_la_transicion_mas_reciente(df: pd.DataFrame, idx, fecha) -> bool:
    """True si `fecha` es >= a todas las fechas de estado ya cargadas en la
    fila, es decir, si el evento que trae esa fecha es el que determina el
    estado_actual/estado_anterior de la fila."""
    fechas_existentes = [
        df.at[idx, columna]
        for columna in COLUMNA_FECHA_POR_ESTADO.values()
        if pd.notna(df.at[idx, columna])
    ]
    if not fechas_existentes:
        return True
    return fecha is not None and fecha >= max(fechas_existentes)


# --- Aplicar eventos (Creado / Actualizado) sobre el DataFrame ------------

def upsert_creado(df: pd.DataFrame, data: dict) -> pd.DataFrame:
    """Alta o completado de la fila del reclamo a partir de un ReclamoCreado.
    Un reclamo solo tiene un evento de creacion valido: si la fila ya tiene
    fecha_creado cargada, cualquier ReclamoCreado adicional es una
    reentrega (o una correccion tardia del mismo evento) y se ignora, sin
    pisar titulo/categoria/direccion/etc. ya cargados.

    El desempate usa el createdAt del propio evento (se queda con el mas
    antiguo), no el orden en que se procesan los blobs: ese orden depende
    de blob.last_modified, que puede empatar (o desempatar por nombre de
    blob, esencialmente al azar) cuando dos ReclamoCreado del mismo
    reclamoId se suben casi al mismo tiempo.

    Si ya existia una fila (por haber llegado antes un ReclamoActualizado),
    tampoco pisa las fechas de estado que ya estaban cargadas."""
    reclamo_id = data.get("reclamoId")
    columna_recibido = COLUMNA_FECHA_POR_ESTADO["RECIBIDO"]
    fecha_creado = _epoch_ms_a_datetime(data.get("createdAt"))

    if reclamo_id in df["reclamoId"].values:
        idx = df.index[df["reclamoId"] == reclamo_id][0]
        fecha_creado_actual = df.at[idx, columna_recibido]
        if pd.notna(fecha_creado_actual):
            if fecha_creado is None or fecha_creado >= fecha_creado_actual:
                logging.info(f"ReclamoCreado duplicado para {reclamo_id}, se ignora.")
                return df
            logging.info(
                f"ReclamoCreado duplicado para {reclamo_id} con createdAt mas antiguo "
                "que el ya aplicado: reemplaza los datos base."
            )
    else:
        df = pd.concat([df, pd.DataFrame([_fila_nueva(reclamo_id)])], ignore_index=True)

    idx = df.index[df["reclamoId"] == reclamo_id][0]

    for col in COLUMNAS_BASE:
        if col != "reclamoId":
            df.at[idx, col] = data.get(col)

    if _es_la_transicion_mas_reciente(df, idx, fecha_creado):
        # RECIBIDO por creacion no tiene estado anterior.
        df.at[idx, "estado_anterior"] = None
    df.at[idx, columna_recibido] = fecha_creado

    _actualizar_derivados(df, idx)
    return df


def aplicar_actualizado(df: pd.DataFrame, data: dict, event_id: str | None = None) -> pd.DataFrame:
    """Aplica un ReclamoActualizado: completa fecha_<estadoNuevo> en la fila
    del reclamo (creando una fila placeholder si el ReclamoCreado todavia no
    llego).

    A diferencia de ReclamoCreado, un reclamo SI puede volver a pasar por un
    estado ya visitado (ej. EN_REVISION -> RECIBIDO esta documentado como
    valido), asi que "ese estado ya tiene fecha" no alcanza para detectar un
    duplicado sin generar falsos positivos. En cambio, se lleva la lista de
    eventId ya aplicados: si el mismo eventId llega de nuevo (reentrega real
    del evento), se ignora; un evento distinto que revisita el mismo estado
    se sigue aplicando con normalidad."""
    reclamo_id = data.get("reclamoId")
    estado_nuevo = data.get("estadoNuevo")
    fecha = _epoch_ms_a_datetime(data.get("updatedAt"))

    columna_fecha = COLUMNA_FECHA_POR_ESTADO.get(estado_nuevo)
    if columna_fecha is None:
        logging.warning(f"estadoNuevo desconocido '{estado_nuevo}' en reclamo {reclamo_id}")
        return df

    if reclamo_id not in df["reclamoId"].values:
        df = pd.concat([df, pd.DataFrame([_fila_nueva(reclamo_id)])], ignore_index=True)

    idx = df.index[df["reclamoId"] == reclamo_id][0]

    eventos_aplicados = df.at[idx, "eventos_actualizado_aplicados"]
    if not isinstance(eventos_aplicados, list):
        eventos_aplicados = []
    if event_id is not None and event_id in eventos_aplicados:
        logging.info(f"ReclamoActualizado duplicado (eventId {event_id}) para {reclamo_id}, se ignora.")
        return df

    if _es_la_transicion_mas_reciente(df, idx, fecha):
        df.at[idx, "estado_anterior"] = data.get("estadoAnterior")

    fecha_actual = df.at[idx, columna_fecha]
    if pd.isna(fecha_actual) or (fecha is not None and fecha >= fecha_actual):
        df.at[idx, columna_fecha] = fecha

    if event_id is not None:
        df.at[idx, "eventos_actualizado_aplicados"] = eventos_aplicados + [event_id]

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
    return _get_container_client(SILVER_CONTAINER_NAME).get_blob_client(TABLA_RECLAMOS_BLOB)


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
    """Aplica un evento de reclamos (Creado o Actualizado) al DataFrame.
    Devuelve None si el eventType no es reconocido."""
    data = evento.get("data", {})
    metadata = evento.get("metadata", {})
    event_type = metadata.get("eventType", "")
    tipo_evento = event_type.split(".")[-1]

    if tipo_evento == "ReclamoCreado":
        return upsert_creado(df, data)
    if tipo_evento == "ReclamoActualizado":
        return aplicar_actualizado(df, data, event_id=metadata.get("eventId"))

    logging.warning(f"eventType inesperado para reclamos: {event_type!r}")
    return None


# --- Trigger: corre 1 vez por dia -----------------------------------------

@bp.timer_trigger(
    schedule="0 0 0 * * *",
    arg_name="timer",
    run_on_startup=False,
    use_monitor=False,
)
def reclamos_bronze_a_silver(timer: func.TimerRequest):
    """Corre una vez por dia. Lee de bronze/Reclamos/ solo los blobs subidos
    despues del checkpoint de la corrida anterior (blobs.last_modified, que
    pone el propio storage) y los aplica sobre la tabla de silver existente."""
    checkpoint = leer_checkpoint()
    logging.info(f"Corrida diaria de reclamos bronze->silver. Checkpoint anterior: {checkpoint}")

    bronze_container = get_bronze_container_client()
    blobs_nuevos = [
        blob for blob in bronze_container.list_blobs(name_starts_with=PREFIJO_RECLAMOS)
        if checkpoint is None or blob.last_modified > checkpoint
    ]
    blobs_nuevos.sort(key=lambda blob: blob.last_modified)

    if not blobs_nuevos:
        logging.info("No hay reclamos nuevos ni actualizados desde la ultima corrida.")
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
