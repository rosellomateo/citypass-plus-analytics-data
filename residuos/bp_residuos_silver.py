"""Capa silver de gestion de residuos inteligente (intervenciones, Grupo 6).

Timer trigger diario que lee los eventos crudos de bronze/Gestion de
Residuos Inteligente/ (IntervencionProgramada y IntervencionActualizada) y
arma una tabla en silver/Gestion de Residuos Inteligente/intervenciones.parquet:
una fila por intervencion (join por intervencionId), con el historial de
fechas por estado.

Los eventos AlertaGenerada/RecoleccionCompletada que este archivo manejaba
antes eran una suposicion de antes de confirmar el bus real (nunca llegaron
a tener datos reales en bronze, nunca se escucharon esos topicos). Los
eventos reales que publica Residuos para este flujo son
IntervencionProgramada/IntervencionActualizada.

correlationId NO es la clave de relacion entre los dos eventos (a diferencia
de Emergencias): aca correlationId apunta a la entidad externa que origino
la intervencion (el emergenciaId, o "act-<actividadId>"/"res-<reservaId>" de
las actividades masivas) y el MISMO correlationId puede repetirse en varias
intervenciones distintas (varios camiones enviados al mismo caso -- lo
confirmamos con datos reales: hasta 5 intervenciones comparten un
correlationId). La clave real es intervencionId, que identifica la parada
en si ("para Analitica, cada parada es una intervencion").

Estados observados hoy: PROGRAMADA (por creacion) -> AFECTADA (unica
transicion que manda IntervencionActualizada, nunca se revisita). Se
modela con el mismo esqueleto ESTADOS/COLUMNA_FECHA_POR_ESTADO que el resto
de los dominios, pero NO con su misma regla de "la fecha mas reciente
manda": programadaEn es una fecha agendada (puede quedar en el futuro, o
ser posterior a cuando se registro la afectacion), no el momento en que
ocurrio una transicion. Por eso estado_actual sale del orden de ESTADOS
(el mas avanzado con fecha cargada), no de comparar fechas -- ver
_actualizar_estado_actual.

Procesa solo lo nuevo desde la ultima corrida (checkpoint mas abajo). Para
reprocesar todo el historico usar scripts/reprocesar_residuos_silver.py.
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

# --- Configuracion: containers, paths de blobs ----------------------------

BRONZE_CONTAINER_NAME = "bronze"
SILVER_CONTAINER_NAME = "silver"
PREFIJO_RESIDUOS = "Gestion de Residuos Inteligente/"
TABLA_INTERVENCIONES_BLOB = "Gestion de Residuos Inteligente/intervenciones.parquet"
CHECKPOINT_BLOB = "Gestion de Residuos Inteligente/_checkpoint.json"

ESTADOS = ["PROGRAMADA", "AFECTADA"]
COLUMNA_FECHA_POR_ESTADO = {estado: f"fecha_{estado.lower()}" for estado in ESTADOS}
ESTADO_POR_COLUMNA_FECHA = {columna: estado for estado, columna in COLUMNA_FECHA_POR_ESTADO.items()}

COLUMNAS_BASE = [
    "intervencionId",
    "emergenciaId",
    "contenedorId",
    "latitud",
    "longitud",
    "camionId",
    "alertaId",
    "causaId",
    "causaTipo",
    "correlationId",
]

COLUMNAS = [
    *COLUMNAS_BASE,
    *COLUMNA_FECHA_POR_ESTADO.values(),
    "estado_actual",
    "estado_anterior",
    "afectada",
]


# --- Helpers para armar/actualizar una fila de la tabla -------------------

def _epoch_ms_a_datetime(epoch_ms):
    """programadaEn/occurredAt vienen en epoch milliseconds."""
    if epoch_ms is None:
        return None
    try:
        return datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc)
    except (TypeError, ValueError, OSError):
        logging.warning(f"programadaEn/occurredAt no es un epoch valido: {epoch_ms!r}")
        return None


def _iso_a_datetime(iso_str):
    """updatedAt viene en ISO-8601."""
    if iso_str is None:
        return None
    try:
        return pd.to_datetime(iso_str, utc=True)
    except (ValueError, TypeError):
        logging.warning(f"No se pudo parsear como fecha: {iso_str!r}")
        return None


def dataframe_vacio() -> pd.DataFrame:
    """Tabla vacia con el esquema completo ya tipado, para arrancar desde
    cero o cuando todavia no existe el parquet en silver."""
    df = pd.DataFrame(columns=COLUMNAS)
    for col in COLUMNA_FECHA_POR_ESTADO.values():
        df[col] = pd.to_datetime(df[col], utc=True)
    df["afectada"] = df["afectada"].astype(bool)
    return df


def _fila_nueva(intervencion_id: str) -> dict:
    """Fila placeholder para un intervencionId que todavia no tiene datos
    (se usa tanto para una IntervencionProgramada nueva como para una
    IntervencionActualizada que llega antes que su IntervencionProgramada --
    visto en produccion, pasa seguido)."""
    fila = {col: None for col in COLUMNAS}
    fila["intervencionId"] = intervencion_id
    fila["afectada"] = False
    return fila


def _actualizar_estado_actual(df: pd.DataFrame, idx) -> None:
    """estado_actual = el estado mas avanzado conocido en la secuencia
    PROGRAMADA -> AFECTADA.

    A diferencia de Reclamos/Emergencias, ACA NO sirve "la fecha mas
    reciente gana": fecha_programada es una fecha agendada (programadaEn),
    no el momento en que ocurrio una transicion -- y puede ser posterior a
    fecha_afectada si la afectacion ocurrio antes de la visita programada
    (pasa seguido en produccion). Por eso se recorre ESTADOS al reves y se
    devuelve el primero con fecha cargada."""
    for estado in reversed(ESTADOS):
        if pd.notna(df.at[idx, COLUMNA_FECHA_POR_ESTADO[estado]]):
            df.at[idx, "estado_actual"] = estado
            return
    df.at[idx, "estado_actual"] = None


def _actualizar_derivados(df: pd.DataFrame, idx) -> None:
    df.at[idx, "afectada"] = pd.notna(df.at[idx, COLUMNA_FECHA_POR_ESTADO["AFECTADA"]])
    _actualizar_estado_actual(df, idx)


# --- Aplicar eventos (Programada / Actualizada) sobre el DataFrame --------

def upsert_programada(df: pd.DataFrame, data: dict) -> pd.DataFrame:
    """Alta de la fila a partir de una IntervencionProgramada. Si la
    intervencion ya tiene fecha_programada cargada, es un evento duplicado:
    se ignora."""
    intervencion_id = data.get("intervencionId")
    columna_programada = COLUMNA_FECHA_POR_ESTADO["PROGRAMADA"]

    if intervencion_id in df["intervencionId"].values:
        idx = df.index[df["intervencionId"] == intervencion_id][0]
        if pd.notna(df.at[idx, columna_programada]):
            logging.info(f"IntervencionProgramada duplicada para {intervencion_id}, se ignora.")
            return df
    else:
        df = pd.concat([df, pd.DataFrame([_fila_nueva(intervencion_id)])], ignore_index=True)

    idx = df.index[df["intervencionId"] == intervencion_id][0]

    for col in COLUMNAS_BASE:
        if col != "intervencionId":
            df.at[idx, col] = data.get(col)

    df.at[idx, columna_programada] = _epoch_ms_a_datetime(data.get("programadaEn"))
    # Si la IntervencionActualizada (AFECTADA) ya llego antes -- out of
    # order, pasa seguido -- no pisamos el estado_anterior que ya puso: la
    # intervencion sigue siendo AFECTADA con PROGRAMADA como anterior.
    if pd.isna(df.at[idx, COLUMNA_FECHA_POR_ESTADO["AFECTADA"]]):
        df.at[idx, "estado_anterior"] = None

    _actualizar_derivados(df, idx)
    return df


def aplicar_actualizada(df: pd.DataFrame, data: dict) -> pd.DataFrame:
    """Aplica una IntervencionActualizada (creando una fila placeholder si
    la IntervencionProgramada todavia no llego). Si la intervencion ya
    tiene fecha cargada para ese estadoNuevo puntual, es un evento
    duplicado: se ignora.

    A diferencia de Reclamos/Emergencias no hace falta "es la transicion
    mas reciente": en los datos reales PROGRAMADA -> AFECTADA es la unica
    transicion posible (nunca se revisita un estado), asi que estos campos
    se aplican directo, sin comparar fechas."""
    intervencion_id = data.get("intervencionId")
    estado_nuevo = data.get("estadoNuevo")

    columna_fecha = COLUMNA_FECHA_POR_ESTADO.get(estado_nuevo)
    if columna_fecha is None:
        logging.warning(f"estadoNuevo desconocido '{estado_nuevo}' en intervencion {intervencion_id}")
        return df

    if intervencion_id in df["intervencionId"].values:
        idx = df.index[df["intervencionId"] == intervencion_id][0]
        if pd.notna(df.at[idx, columna_fecha]):
            logging.info(f"IntervencionActualizada duplicada (estado {estado_nuevo}) para {intervencion_id}, se ignora.")
            return df
    else:
        df = pd.concat([df, pd.DataFrame([_fila_nueva(intervencion_id)])], ignore_index=True)

    idx = df.index[df["intervencionId"] == intervencion_id][0]

    df.at[idx, "estado_anterior"] = data.get("estadoAnterior")
    # camionId se asigna recien al despachar: viene null en Programada y
    # completo aca. causaId/causaTipo los repite el productor, pero por si
    # alguna vez vienen distintos nos quedamos con el valor no nulo.
    df.at[idx, "camionId"] = data.get("camionId")
    if data.get("causaId") is not None:
        df.at[idx, "causaId"] = data.get("causaId")
    if data.get("causaTipo") is not None:
        df.at[idx, "causaTipo"] = data.get("causaTipo")

    df.at[idx, columna_fecha] = _iso_a_datetime(data.get("updatedAt"))

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
    return _get_container_client(SILVER_CONTAINER_NAME).get_blob_client(TABLA_INTERVENCIONES_BLOB)


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
    """Aplica un evento de residuos (Programada o Actualizada) al
    DataFrame. Devuelve None si el eventType no es reconocido."""
    data = evento.get("data", {})
    event_type = evento.get("metadata", {}).get("eventType", "")
    tipo_evento = event_type.split(".")[-1]

    if tipo_evento == "IntervencionProgramada":
        return upsert_programada(df, data)
    if tipo_evento == "IntervencionActualizada":
        return aplicar_actualizada(df, data)

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
    """Corre una vez por dia. Lee de bronze/Gestion de Residuos
    Inteligente/ solo los blobs subidos despues del checkpoint de la
    corrida anterior (blobs.last_modified, que pone el propio storage) y
    los aplica sobre la tabla de silver existente."""
    checkpoint = leer_checkpoint()
    logging.info(f"Corrida diaria de residuos bronze->silver. Checkpoint anterior: {checkpoint}")

    bronze_container = get_bronze_container_client()
    blobs_nuevos = [
        blob for blob in bronze_container.list_blobs(name_starts_with=PREFIJO_RESIDUOS)
        if checkpoint is None or blob.last_modified > checkpoint
    ]
    blobs_nuevos.sort(key=lambda blob: blob.last_modified)

    if not blobs_nuevos:
        logging.info("No hay intervenciones nuevas ni actualizadas desde la ultima corrida.")
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
