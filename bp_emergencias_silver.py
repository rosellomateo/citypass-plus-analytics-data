"""Capa silver de emergencias y seguridad.

Timer trigger diario que lee los eventos crudos de bronze/Emergencias y
Seguridad/ (EmergenciaCreada y EmergenciaActualizada) y arma una tabla en
silver/Emergencias y Seguridad/emergencias.parquet: una fila por emergencia,
con el historial de fechas por estado del ciclo PENDIENTE -> VALIDADA ->
DESPACHADA -> EN_CAMINO -> EN_LUGAR -> RESUELTA -> CERRADA (+ rama
DESCARTADA desde PENDIENTE/VALIDADA), igual que bp_reclamos_silver.py.

Diferencias con reclamos:
  - Cada EmergenciaActualizada tambien trae quien la atendio (usuarioId),
    a que movil se asigno (asignadoA), que area responde (areaResponsable)
    y el resultado (resolucion) — se guardan como "ultimo valor conocido",
    actualizados solo cuando esa actualizacion es la transicion mas
    reciente de la emergencia (misma regla que estado_actual/estado_anterior).
  - La fecha de una transicion usa resueltoAt cuando viene (solo en RESUELTA
    o DESCARTADA), y si no updatedAt.
  - A diferencia de reclamos, aca cada evento SI valida duplicados: si la
    emergencia ya tiene fecha para ese estado puntual, se ignora el evento
    (misma logica que bp_movilidad_silver.py / bp_residuos_silver.py).

Procesa solo lo nuevo desde la ultima corrida (checkpoint mas abajo). Para
reprocesar todo el historico usar scripts/reprocesar_emergencias_silver.py.
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
PREFIJO_EMERGENCIAS = "Emergencias y Seguridad/"
TABLA_EMERGENCIAS_BLOB = "Emergencias y Seguridad/emergencias.parquet"
CHECKPOINT_BLOB = "Emergencias y Seguridad/_checkpoint.json"

# Ciclo de estados de una emergencia (ver README del Grupo 5). DESCARTADA es
# una rama aparte, solo alcanzable desde PENDIENTE/VALIDADA.
ESTADOS = [
    "PENDIENTE",
    "VALIDADA",
    "DESPACHADA",
    "EN_CAMINO",
    "EN_LUGAR",
    "RESUELTA",
    "CERRADA",
    "DESCARTADA",
]

COLUMNA_FECHA_POR_ESTADO = {estado: f"fecha_{estado.lower()}" for estado in ESTADOS}
# Excepcion de nombre, igual que en reclamos: PENDIENTE se guarda como fecha_creada.
COLUMNA_FECHA_POR_ESTADO["PENDIENTE"] = "fecha_creada"
ESTADO_POR_COLUMNA_FECHA = {columna: estado for estado, columna in COLUMNA_FECHA_POR_ESTADO.items()}

COLUMNAS_BASE = [
    "emergenciaId",
    "ciudadanoId",
    "titulo",
    "descripcion",
    "categoria",
    "direccion",
    "barrio",
    "latitud",
    "longitud",
    "correlationId",
]

COLUMNAS = [
    *COLUMNAS_BASE,
    *COLUMNA_FECHA_POR_ESTADO.values(),
    "prioridad",
    "estado_actual",
    "estado_anterior",
    "usuarioId",
    "asignadoA",
    "areaResponsable",
    "resolucion",
    "resuelta",
    "descartada",
    "cerrada",
]


# --- Helpers para armar/actualizar una fila de la tabla -------------------

def _iso_a_datetime(iso_str):
    """createdAt/updatedAt/resueltoAt vienen en ISO-8601; los pasamos a
    Timestamp UTC (pd.to_datetime tolera con/sin offset y el sufijo 'Z')."""
    if iso_str is None:
        return None
    return pd.to_datetime(iso_str, utc=True)


def dataframe_vacio() -> pd.DataFrame:
    """Tabla vacia con el esquema completo ya tipado, para arrancar desde
    cero o cuando todavia no existe el parquet en silver."""
    df = pd.DataFrame(columns=COLUMNAS)
    for col in COLUMNA_FECHA_POR_ESTADO.values():
        df[col] = pd.to_datetime(df[col], utc=True)
    for col in ("resuelta", "descartada", "cerrada"):
        df[col] = df[col].astype(bool)
    return df


def _fila_nueva(emergencia_id: str) -> dict:
    """Fila placeholder para un emergenciaId que todavia no tiene datos (se
    usa tanto para una EmergenciaCreada nueva como para una
    EmergenciaActualizada que llega antes que su EmergenciaCreada)."""
    fila = {col: None for col in COLUMNAS}
    fila["emergenciaId"] = emergencia_id
    fila["resuelta"] = False
    fila["descartada"] = False
    fila["cerrada"] = False
    return fila


def _actualizar_estado_actual(df: pd.DataFrame, idx) -> None:
    """estado_actual = el estado cuya fecha es la mas reciente de todas las
    cargadas en la fila. No depende del orden de llegada de los eventos,
    solo de las fechas que traen."""
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
    df.at[idx, "resuelta"] = pd.notna(df.at[idx, COLUMNA_FECHA_POR_ESTADO["RESUELTA"]])
    df.at[idx, "descartada"] = pd.notna(df.at[idx, COLUMNA_FECHA_POR_ESTADO["DESCARTADA"]])
    df.at[idx, "cerrada"] = pd.notna(df.at[idx, COLUMNA_FECHA_POR_ESTADO["CERRADA"]])
    _actualizar_estado_actual(df, idx)


def _es_la_transicion_mas_reciente(df: pd.DataFrame, idx, fecha) -> bool:
    """True si `fecha` es >= a todas las fechas de estado ya cargadas en la
    fila, es decir, si el evento que trae esa fecha es el que determina el
    estado_actual/estado_anterior (y la "foto" operativa: usuarioId,
    asignadoA, areaResponsable, resolucion) de la fila."""
    fechas_existentes = [
        df.at[idx, columna]
        for columna in COLUMNA_FECHA_POR_ESTADO.values()
        if pd.notna(df.at[idx, columna])
    ]
    if not fechas_existentes:
        return True
    return fecha is not None and fecha >= max(fechas_existentes)


# --- Aplicar eventos (Creada / Actualizada) sobre el DataFrame ------------

def upsert_creada(df: pd.DataFrame, data: dict) -> pd.DataFrame:
    """Alta de la fila a partir de una EmergenciaCreada. Si la emergencia ya
    tiene fecha_creada cargada, es un evento duplicado: se ignora. Si una
    EmergenciaActualizada ya llego antes (fuera de orden), no pisa la
    prioridad ni el estado_anterior ya conocidos."""
    emergencia_id = data.get("emergenciaId")

    if emergencia_id in df["emergenciaId"].values:
        idx = df.index[df["emergenciaId"] == emergencia_id][0]
        if pd.notna(df.at[idx, "fecha_creada"]):
            logging.info(f"EmergenciaCreada duplicada para {emergencia_id}, se ignora.")
            return df
    else:
        df = pd.concat([df, pd.DataFrame([_fila_nueva(emergencia_id)])], ignore_index=True)

    idx = df.index[df["emergenciaId"] == emergencia_id][0]

    for col in COLUMNAS_BASE:
        if col != "emergenciaId":
            df.at[idx, col] = data.get(col)

    if pd.isna(df.at[idx, "prioridad"]):
        df.at[idx, "prioridad"] = data.get("prioridad")

    fecha_creada = _iso_a_datetime(data.get("createdAt"))
    if _es_la_transicion_mas_reciente(df, idx, fecha_creada):
        # PENDIENTE por creacion no tiene estado anterior.
        df.at[idx, "estado_anterior"] = None
    df.at[idx, "fecha_creada"] = fecha_creada

    _actualizar_derivados(df, idx)
    return df


def aplicar_actualizada(df: pd.DataFrame, data: dict) -> pd.DataFrame:
    """Aplica una EmergenciaActualizada (creando una fila placeholder si la
    EmergenciaCreada todavia no llego). Si la emergencia ya tiene fecha
    cargada para ese estadoNuevo puntual, es un evento duplicado: se ignora."""
    emergencia_id = data.get("emergenciaId")
    estado_nuevo = data.get("estadoNuevo")

    columna_fecha = COLUMNA_FECHA_POR_ESTADO.get(estado_nuevo)
    if columna_fecha is None:
        logging.warning(f"estadoNuevo desconocido '{estado_nuevo}' en emergencia {emergencia_id}")
        return df

    if emergencia_id in df["emergenciaId"].values:
        idx = df.index[df["emergenciaId"] == emergencia_id][0]
        if pd.notna(df.at[idx, columna_fecha]):
            logging.info(f"EmergenciaActualizada duplicada (estado {estado_nuevo}) para {emergencia_id}, se ignora.")
            return df
    else:
        df = pd.concat([df, pd.DataFrame([_fila_nueva(emergencia_id)])], ignore_index=True)

    idx = df.index[df["emergenciaId"] == emergencia_id][0]

    # resueltoAt es mas preciso que updatedAt para RESUELTA/DESCARTADA; si
    # no viene (otros estados), usamos updatedAt.
    fecha = _iso_a_datetime(data.get("resueltoAt")) or _iso_a_datetime(data.get("updatedAt"))

    if _es_la_transicion_mas_reciente(df, idx, fecha):
        df.at[idx, "estado_anterior"] = data.get("estadoAnterior")
        df.at[idx, "usuarioId"] = data.get("usuarioId")
        df.at[idx, "asignadoA"] = data.get("asignadoA")
        df.at[idx, "areaResponsable"] = data.get("areaResponsable")
        df.at[idx, "resolucion"] = data.get("resolucion")
        if data.get("prioridad") is not None:
            df.at[idx, "prioridad"] = data.get("prioridad")

    df.at[idx, columna_fecha] = fecha

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
    return _get_container_client(SILVER_CONTAINER_NAME).get_blob_client(TABLA_EMERGENCIAS_BLOB)


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
    """Aplica un evento de emergencias (Creada o Actualizada) al DataFrame.
    Devuelve None si el eventType no es reconocido."""
    data = evento.get("data", {})
    event_type = evento.get("metadata", {}).get("eventType", "")
    tipo_evento = event_type.split(".")[-1]

    if tipo_evento == "EmergenciaCreada":
        return upsert_creada(df, data)
    if tipo_evento == "EmergenciaActualizada":
        return aplicar_actualizada(df, data)

    logging.warning(f"eventType inesperado para emergencias: {event_type!r}")
    return None


# --- Trigger: corre 1 vez por dia -----------------------------------------

@bp.timer_trigger(
    schedule="0 0 0 * * *",
    arg_name="timer",
    run_on_startup=False,
    use_monitor=False,
)
def emergencias_bronze_a_silver(timer: func.TimerRequest):
    """Corre una vez por dia. Lee de bronze/Emergencias y Seguridad/ solo
    los blobs subidos despues del checkpoint de la corrida anterior
    (blobs.last_modified, que pone el propio storage) y los aplica sobre la
    tabla de silver existente."""
    checkpoint = leer_checkpoint()
    logging.info(f"Corrida diaria de emergencias bronze->silver. Checkpoint anterior: {checkpoint}")

    bronze_container = get_bronze_container_client()
    blobs_nuevos = [
        blob for blob in bronze_container.list_blobs(name_starts_with=PREFIJO_EMERGENCIAS)
        if checkpoint is None or blob.last_modified > checkpoint
    ]
    blobs_nuevos.sort(key=lambda blob: blob.last_modified)

    if not blobs_nuevos:
        logging.info("No hay emergencias nuevas ni actualizadas desde la ultima corrida.")
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
