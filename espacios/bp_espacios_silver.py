"""Capa silver de espacios publicos y cultura.

Timer trigger diario que lee los eventos crudos de bronze/Espacios Publicos
y Cultura/ y arma tres tablas en silver/Espacios Publicos y Cultura/, cada
una con su propio checkpoint:

  - actividades.parquet (ActividadCreada / ActividadModificada): una fila
    por actividad (evento con cupo). Casos 1, 3 y 6.
  - espacios_afectados.parquet (EspacioPublicoAfectado): una fila por
    afectacion de un espacio fisico. Casos 2 (causaTipo RECLAMO) y 4
    (INCIDENTE_VIAL).
  - reservas.parquet (ReservaCreada / ReservaActualizada / ReservaAfectada):
    una fila por reserva. Caso 6 (reservas de ciudadanos vs. eventos
    totales) y caso 5 (reservas afectadas por una emergencia en bici, A10
    del CU-01).

NOTA (14/nov): el flujo de Reservas se habia abandonado porque nunca se
suscribio en bronze. El 18/nov se retomo: Espacios publica ReservaCreada /
ReservaActualizada / ReservaAfectada y los casos 5 y 6 los necesitan.

Procesa solo lo nuevo desde la ultima corrida (checkpoint mas abajo), mismo
patron que bp_reclamos_silver.py / bp_movilidad_silver.py / bp_residuos_silver.py.
Para reprocesar todo el historico usar scripts/reprocesar_espacios_actividades_silver.py.
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

from ._barrios import barrio_desde_coordenadas

bp = func.Blueprint()

# --- Configuracion: containers, paths de blobs ----------------------------

BRONZE_CONTAINER_NAME = "bronze"
SILVER_CONTAINER_NAME = "silver"
PREFIJO_ESPACIOS = "Espacios Publicos y Cultura/"


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


# ===========================================================================
# Actividades (ActividadCreada / ActividadModificada)
# ===========================================================================

TABLA_ACTIVIDADES_BLOB = "Espacios Publicos y Cultura/actividades.parquet"
CHECKPOINT_ACTIVIDADES_BLOB = "Espacios Publicos y Cultura/_checkpoint_actividades.json"

COLUMNAS_BASE_ACTIVIDAD = [
    "actividadId",
    "titulo",
    "capacidad",
    "fechaHoraInicio",
    "fechaHoraFinalizacion",
    "latitud",
    "longitud",
    "direccion",
    "espacioNombre",
    "categoria",
    "actorSub",
    "correlationId",
    # Nuevo en ActividadCreada.v3 (14/nov): CIUDADANO o CIUDAD. v2 no lo
    # trae -- queda null para las actividades creadas con ese esquema viejo.
    "organizadorTipo",
    # El evento no trae barrio (ver MODELADO_EVENTOS_PARTE2.md #2: los
    # eventos nuevos usan lat/lng en vez de zona/barrio) -- se deriva de la
    # misma coordenada del evento por point-in-polygon, ver _barrios.py.
    "barrio",
]

COLUMNAS_ACTIVIDAD = [
    *COLUMNAS_BASE_ACTIVIDAD,
    "estado_actual",
    "fecha_actualizacion",
    "motivo",
    "causaId",
    "causaTipo",
    # capacidad >= 500 -> evento masivo (caso 3). Se calcula una vez al
    # cargar la ActividadCreada, no en gold, para no repetir la regla en
    # cada consumidor de esta tabla.
    "masivo",
    # Contadores por causa: causaId/causaTipo de arriba solo guardan el
    # ultimo conocido (y no se pisan con null), asi que no alcanzan para
    # "cuantas veces" -- una actividad modificada 2 veces por el mismo
    # causaTipo contaria 1 sola vez ahi. INCIDENTE_VIAL y ACCIDENTE_VIAL son
    # valores DISTINTOS vistos en produccion (no es un typo: 18 y 15
    # eventos respectivamente), cada uno con su propia metrica que pide
    # "cantidad de eventos", no "cantidad de actividades distintas".
    "veces_evento_climatico",
    "veces_reclamo",
    "veces_incidente_vial",
    "veces_accidente_vial",
    "eventos_modificada_aplicados",
]

_COLUMNA_CONTADOR_POR_CAUSA = {
    "EVENTO_CLIMATICO": "veces_evento_climatico",
    "RECLAMO": "veces_reclamo",
    "INCIDENTE_VIAL": "veces_incidente_vial",
    "ACCIDENTE_VIAL": "veces_accidente_vial",
}


def _epoch_ms_a_datetime_actividad(epoch_ms):
    """fechaHoraInicio/fechaHoraFinalizacion/modificadaEn vienen en epoch
    milliseconds (timestamp-millis)."""
    if epoch_ms is None:
        return None
    try:
        return pd.to_datetime(epoch_ms, unit="ms", utc=True)
    except (ValueError, TypeError):
        logging.warning(f"Epoch invalido en actividad: {epoch_ms!r}")
        return None


def dataframe_vacio_actividades() -> pd.DataFrame:
    """Tabla vacia con el esquema completo ya tipado, para arrancar desde
    cero o cuando todavia no existe el parquet en silver."""
    df = pd.DataFrame(columns=COLUMNAS_ACTIVIDAD)
    for col in ("fechaHoraInicio", "fechaHoraFinalizacion", "fecha_actualizacion"):
        df[col] = pd.to_datetime(df[col], utc=True)
    for col in _COLUMNA_CONTADOR_POR_CAUSA.values():
        df[col] = df[col].astype(int)
    df["masivo"] = df["masivo"].astype(bool)
    return df


def _fila_nueva_actividad(actividad_id: str) -> dict:
    """Fila placeholder para un actividadId que todavia no tiene datos (se
    usa tanto para una ActividadCreada nueva como para una
    ActividadModificada que llega antes que su ActividadCreada)."""
    fila = {col: None for col in COLUMNAS_ACTIVIDAD}
    fila["actividadId"] = actividad_id
    for columna_contador in _COLUMNA_CONTADOR_POR_CAUSA.values():
        fila[columna_contador] = 0
    fila["masivo"] = False
    fila["eventos_modificada_aplicados"] = []
    return fila


def upsert_actividad_creada(df: pd.DataFrame, data: dict) -> pd.DataFrame:
    """Alta de la fila a partir de una ActividadCreada. El evento no trae un
    timestamp propio de creacion (no hay createdAt), asi que el duplicado se
    detecta por si ya hay titulo cargado. estado_actual arranca con el
    "estado" que manda el propio evento (visto en produccion: casi siempre
    PROGRAMADA) -- a diferencia de Reclamos/Emergencias, aca no lo
    recalculamos nosotros porque los estados no son un enum fijo y conocido
    de antemano."""
    actividad_id = data.get("actividadId")

    if actividad_id in df["actividadId"].values:
        idx = df.index[df["actividadId"] == actividad_id][0]
        if pd.notna(df.at[idx, "titulo"]):
            logging.info(f"ActividadCreada duplicada para {actividad_id}, se ignora.")
            return df
    else:
        df = pd.concat([df, pd.DataFrame([_fila_nueva_actividad(actividad_id)])], ignore_index=True)

    idx = df.index[df["actividadId"] == actividad_id][0]

    for col in COLUMNAS_BASE_ACTIVIDAD:
        if col != "actividadId":
            df.at[idx, col] = data.get(col)

    ubicacion = data.get("ubicacion") or {}
    df.at[idx, "latitud"] = ubicacion.get("lat")
    df.at[idx, "longitud"] = ubicacion.get("lng")
    df.at[idx, "barrio"] = barrio_desde_coordenadas(ubicacion.get("lat"), ubicacion.get("lng"))
    df.at[idx, "fechaHoraInicio"] = _epoch_ms_a_datetime_actividad(data.get("fechaHoraInicio"))
    df.at[idx, "fechaHoraFinalizacion"] = _epoch_ms_a_datetime_actividad(data.get("fechaHoraFinalizacion"))
    df.at[idx, "masivo"] = (data.get("capacidad") or 0) >= 500

    if pd.isna(df.at[idx, "estado_actual"]):
        df.at[idx, "estado_actual"] = data.get("estado")

    return df


def aplicar_actividad_modificada(df: pd.DataFrame, data: dict, event_id: str | None = None) -> pd.DataFrame:
    """Aplica una ActividadModificada (creando una fila placeholder si la
    ActividadCreada todavia no llego). A diferencia de Residuos, una
    actividad SI puede revisitar un estado (ej. PROGRAMADA -> SUSPENDIDA ->
    PROGRAMADA, visto en produccion), asi que estado_actual/fecha_actualizacion
    se actualizan con la transicion de modificadaEn mas reciente, no con "la
    primera que llega" ni con una lista fija de estados.

    Los contadores veces_<causa> (para metricas tipo "cantidad de eventos",
    a diferencia de "cantidad de actividades distintas") se incrementan
    siempre que la causa venga, sin importar si esta transicion termina
    siendo la mas reciente -- por eso necesitan su propio dedup por eventId
    (si no, una reentrega de Kafka los contaria dos veces; mismo mecanismo
    que bp_reclamos_silver.py)."""
    actividad_id = data.get("actividadId")

    if actividad_id not in df["actividadId"].values:
        df = pd.concat([df, pd.DataFrame([_fila_nueva_actividad(actividad_id)])], ignore_index=True)

    idx = df.index[df["actividadId"] == actividad_id][0]

    eventos_aplicados = df.at[idx, "eventos_modificada_aplicados"]
    if not isinstance(eventos_aplicados, list):
        eventos_aplicados = []
    if event_id is not None and event_id in eventos_aplicados:
        logging.info(f"ActividadModificada duplicada (eventId {event_id}) para {actividad_id}, se ignora.")
        return df

    causa_tipo = data.get("causaTipo")
    columna_contador = _COLUMNA_CONTADOR_POR_CAUSA.get(causa_tipo)
    if columna_contador is not None:
        df.at[idx, columna_contador] = df.at[idx, columna_contador] + 1

    if event_id is not None:
        df.at[idx, "eventos_modificada_aplicados"] = eventos_aplicados + [event_id]

    fecha = _epoch_ms_a_datetime_actividad(data.get("modificadaEn"))
    fecha_actual = df.at[idx, "fecha_actualizacion"]

    if pd.isna(fecha_actual) or (fecha is not None and fecha >= fecha_actual):
        df.at[idx, "estado_actual"] = data.get("estadoNuevo")
        df.at[idx, "fecha_actualizacion"] = fecha
        df.at[idx, "motivo"] = data.get("motivo")
        if data.get("causaId") is not None:
            df.at[idx, "causaId"] = data.get("causaId")
        if causa_tipo is not None:
            df.at[idx, "causaTipo"] = causa_tipo

    return df


def get_blob_client_actividades():
    return _get_container_client(SILVER_CONTAINER_NAME).get_blob_client(TABLA_ACTIVIDADES_BLOB)


def leer_checkpoint_actividades() -> datetime | None:
    blob_client = _get_container_client(SILVER_CONTAINER_NAME).get_blob_client(CHECKPOINT_ACTIVIDADES_BLOB)
    try:
        contenido = blob_client.download_blob().readall()
    except ResourceNotFoundError:
        return None
    return datetime.fromisoformat(json.loads(contenido)["ultima_corrida"])


def escribir_checkpoint_actividades(momento: datetime) -> None:
    blob_client = _get_container_client(SILVER_CONTAINER_NAME).get_blob_client(CHECKPOINT_ACTIVIDADES_BLOB)
    contenido = json.dumps({"ultima_corrida": momento.isoformat()})
    blob_client.upload_blob(contenido, overwrite=True)


def leer_tabla_actividades(blob_client) -> pd.DataFrame:
    try:
        contenido = blob_client.download_blob().readall()
    except ResourceNotFoundError:
        return dataframe_vacio_actividades()
    return pd.read_parquet(BytesIO(contenido))


def aplicar_evento_actividad(df: pd.DataFrame, evento: dict) -> pd.DataFrame | None:
    """Aplica un evento de actividades (Creada o Modificada) al DataFrame.
    Devuelve None si el eventType no es reconocido."""
    data = evento.get("data", {})
    metadata = evento.get("metadata", {})
    event_type = metadata.get("eventType", "")
    tipo_evento = event_type.split(".")[-1]

    if tipo_evento == "ActividadCreada":
        return upsert_actividad_creada(df, data)
    if tipo_evento == "ActividadModificada":
        return aplicar_actividad_modificada(df, data, event_id=metadata.get("eventId"))

    return None


# ===========================================================================
# Helpers comunes: checkpoint + loop sobre los blobs nuevos de bronze
# ===========================================================================

def _iso_o_epoch_a_datetime(valor):
    """Fechas de los eventos de espacios: ISO-8601 (EspacioPublicoAfectado,
    reservas) o epoch millis (timestamp-millis de Avro ya pasado a JSON)."""
    if valor is None:
        return None
    try:
        if isinstance(valor, (int, float)):
            return pd.to_datetime(valor, unit="ms", utc=True)
        return pd.to_datetime(valor, utc=True)
    except (ValueError, TypeError):
        logging.warning(f"Fecha invalida en evento de espacios: {valor!r}")
        return None


def _tipo_evento(evento: dict) -> str:
    """Ultimo segmento del eventType, sin sufijo de version (.v2)."""
    event_type = evento.get("metadata", {}).get("eventType", "")
    partes = event_type.split(".")
    if len(partes) > 4 and partes[-1].startswith("v") and partes[-1][1:].isdigit():
        partes = partes[:-1]
    return partes[-1]


def _leer_checkpoint(nombre_blob: str) -> datetime | None:
    blob_client = _get_container_client(SILVER_CONTAINER_NAME).get_blob_client(nombre_blob)
    try:
        contenido = blob_client.download_blob().readall()
    except ResourceNotFoundError:
        return None
    return datetime.fromisoformat(json.loads(contenido)["ultima_corrida"])


def _escribir_checkpoint(nombre_blob: str, momento: datetime) -> None:
    blob_client = _get_container_client(SILVER_CONTAINER_NAME).get_blob_client(nombre_blob)
    blob_client.upload_blob(json.dumps({"ultima_corrida": momento.isoformat()}), overwrite=True)


def _leer_tabla(nombre_blob: str, vacio, columnas: list[str]) -> pd.DataFrame:
    blob_client = _get_container_client(SILVER_CONTAINER_NAME).get_blob_client(nombre_blob)
    try:
        contenido = blob_client.download_blob().readall()
    except ResourceNotFoundError:
        return vacio()
    df = pd.read_parquet(BytesIO(contenido))
    for col in columnas:
        if col not in df.columns:
            df[col] = None
    return df


def _procesar(nombre: str, tabla_blob: str, checkpoint_blob: str, vacio, columnas, aplicar) -> None:
    """Loop generico: aplica sobre la tabla de silver los blobs de bronze/
    Espacios subidos despues del checkpoint de ESA tabla."""
    checkpoint = _leer_checkpoint(checkpoint_blob)
    logging.info(f"Espacios ({nombre}) bronze->silver. Checkpoint anterior: {checkpoint}")

    bronze_container = get_bronze_container_client()
    blobs_nuevos = [
        blob for blob in bronze_container.list_blobs(name_starts_with=PREFIJO_ESPACIOS)
        if checkpoint is None or blob.last_modified > checkpoint
    ]
    blobs_nuevos.sort(key=lambda blob: blob.last_modified)
    if not blobs_nuevos:
        logging.info(f"Espacios ({nombre}): no hay blobs nuevos.")
        return

    df = _leer_tabla(tabla_blob, vacio, columnas)
    procesados = 0
    checkpoint_nuevo = checkpoint
    for blob in blobs_nuevos:
        contenido = bronze_container.download_blob(blob.name).readall()
        try:
            evento = json.loads(contenido)
        except json.JSONDecodeError:
            evento = None
        if isinstance(evento, dict):
            df_actualizado = aplicar(df, evento)
            if df_actualizado is not None:
                df = df_actualizado
                procesados += 1
        if checkpoint_nuevo is None or blob.last_modified > checkpoint_nuevo:
            checkpoint_nuevo = blob.last_modified

    escribir_tabla(_get_container_client(SILVER_CONTAINER_NAME).get_blob_client(tabla_blob), df)
    _escribir_checkpoint(checkpoint_blob, checkpoint_nuevo)
    logging.info(f"Espacios ({nombre}): procesados {procesados}/{len(blobs_nuevos)}. Tabla con {len(df)} filas.")


# ===========================================================================
# Espacios afectados (EspacioPublicoAfectado) -- casos 2 y 4
# ===========================================================================

TABLA_AFECTADOS_BLOB = "Espacios Publicos y Cultura/espacios_afectados.parquet"
CHECKPOINT_AFECTADOS_BLOB = "Espacios Publicos y Cultura/_checkpoint_afectados.json"

COLUMNAS_AFECTADO = [
    "afectacionId",
    "espacioId",
    "categoria",
    "latitud",
    "longitud",
    "barrio",
    "tipoAfectacion",
    "afectacionDesde",
    "afectacionHastaEstimada",
    "causaId",
    "causaTipo",
    "correlationId",
    "occurredAt",
]
_FECHAS_AFECTADO = ("afectacionDesde", "afectacionHastaEstimada", "occurredAt")


def dataframe_vacio_afectados() -> pd.DataFrame:
    df = pd.DataFrame(columns=COLUMNAS_AFECTADO)
    for col in _FECHAS_AFECTADO:
        df[col] = pd.to_datetime(df[col], utc=True)
    return df


def aplicar_evento_afectado(df: pd.DataFrame, evento: dict) -> pd.DataFrame | None:
    """Alta de una afectacion de espacio. Una reentrega (mismo afectacionId)
    se ignora. Devuelve None si el evento no es EspacioPublicoAfectado."""
    if _tipo_evento(evento) != "EspacioPublicoAfectado":
        return None
    data = evento.get("data", {}) or {}
    afectacion_id = data.get("afectacionId")
    if afectacion_id is None or afectacion_id in df["afectacionId"].values:
        return df

    fila = {col: data.get(col) for col in COLUMNAS_AFECTADO}
    for col in _FECHAS_AFECTADO:
        fila[col] = _iso_o_epoch_a_datetime(data.get(col))
    fila["barrio"] = barrio_desde_coordenadas(data.get("latitud"), data.get("longitud"))
    return pd.concat([df, pd.DataFrame([fila])], ignore_index=True)


def procesar_afectados() -> None:
    _procesar("espacios afectados", TABLA_AFECTADOS_BLOB, CHECKPOINT_AFECTADOS_BLOB,
              dataframe_vacio_afectados, COLUMNAS_AFECTADO, aplicar_evento_afectado)


# ===========================================================================
# Reservas (ReservaCreada / ReservaActualizada / ReservaAfectada) -- casos 5 y 6
# ===========================================================================

TABLA_RESERVAS_BLOB = "Espacios Publicos y Cultura/reservas.parquet"
CHECKPOINT_RESERVAS_BLOB = "Espacios Publicos y Cultura/_checkpoint_reservas.json"

COLUMNAS_BASE_RESERVA = [
    "reservaId",
    "tipoReserva",
    "recursoId",
    "espacioId",
    "categoria",
    "zona",
    "latitud",
    "longitud",
    "cantidadPersonas",
    "cupoMaximo",
    "organizadorId",
    "correlationId",
]

COLUMNAS_RESERVA = [
    *COLUMNAS_BASE_RESERVA,
    "barrio",
    "fechaHora",
    "fechaFin",
    "createdAt",
    # ReservaCreada no trae estado: arranca PENDIENTE hasta que llega una
    # ReservaActualizada (CONFIRMADA / CANCELADA / ...).
    "estado_actual",
    "fecha_actualizacion",
    # ReservaAfectada (CU-01, A10): la reserva quedo afectada por una
    # emergencia en bici cercana -- caso 5.
    "afectada",
    "afectadaEn",
    "accionAfectacion",
    "emergenciaIdAfectacion",
]
_FECHAS_RESERVA = ("fechaHora", "fechaFin", "createdAt", "fecha_actualizacion", "afectadaEn")


def dataframe_vacio_reservas() -> pd.DataFrame:
    df = pd.DataFrame(columns=COLUMNAS_RESERVA)
    for col in _FECHAS_RESERVA:
        df[col] = pd.to_datetime(df[col], utc=True)
    df["afectada"] = df["afectada"].astype(bool)
    return df


def _fila_reserva(df: pd.DataFrame, reserva_id) -> tuple[pd.DataFrame, int]:
    if reserva_id not in df["reservaId"].values:
        fila = {col: None for col in COLUMNAS_RESERVA}
        fila["reservaId"] = reserva_id
        fila["afectada"] = False
        df = pd.concat([df, pd.DataFrame([fila])], ignore_index=True)
    return df, df.index[df["reservaId"] == reserva_id][0]


def aplicar_evento_reserva(df: pd.DataFrame, evento: dict) -> pd.DataFrame | None:
    """Aplica un evento de reservas. Los eventos pueden llegar fuera de orden
    (se crea un placeholder). Devuelve None si no es un evento de reservas."""
    tipo = _tipo_evento(evento)
    if tipo not in ("ReservaCreada", "ReservaActualizada", "ReservaAfectada"):
        return None
    data = evento.get("data", {}) or {}
    reserva_id = data.get("reservaId")
    if reserva_id is None:
        return df
    df, idx = _fila_reserva(df, reserva_id)

    if tipo == "ReservaCreada":
        for col in COLUMNAS_BASE_RESERVA:
            if data.get(col) is not None:
                df.at[idx, col] = data.get(col)
        df.at[idx, "fechaHora"] = _iso_o_epoch_a_datetime(data.get("fechaHora"))
        df.at[idx, "fechaFin"] = _iso_o_epoch_a_datetime(data.get("fechaFin"))
        df.at[idx, "createdAt"] = _iso_o_epoch_a_datetime(data.get("createdAt"))
        df.at[idx, "barrio"] = barrio_desde_coordenadas(data.get("latitud"), data.get("longitud"))
        if pd.isna(df.at[idx, "estado_actual"]):
            df.at[idx, "estado_actual"] = "PENDIENTE"
        return df

    if tipo == "ReservaActualizada":
        fecha = _iso_o_epoch_a_datetime(data.get("updatedAt"))
        anterior = df.at[idx, "fecha_actualizacion"]
        # La transicion mas reciente manda (los eventos pueden llegar en
        # cualquier orden); una reentrega de la misma no cambia nada.
        if pd.isna(anterior) or (fecha is not None and fecha >= anterior):
            df.at[idx, "estado_actual"] = data.get("estadoNuevo")
            df.at[idx, "fecha_actualizacion"] = fecha
        return df

    # ReservaAfectada
    for col in ("tipoReserva", "espacioId", "correlationId"):
        if pd.isna(df.at[idx, col]) and data.get(col) is not None:
            df.at[idx, col] = data.get(col)
    df.at[idx, "afectada"] = True
    df.at[idx, "afectadaEn"] = _iso_o_epoch_a_datetime(data.get("afectadaEn"))
    df.at[idx, "accionAfectacion"] = data.get("accion")
    df.at[idx, "emergenciaIdAfectacion"] = data.get("emergenciaId")
    return df


def procesar_reservas() -> None:
    _procesar("reservas", TABLA_RESERVAS_BLOB, CHECKPOINT_RESERVAS_BLOB,
              dataframe_vacio_reservas, COLUMNAS_RESERVA, aplicar_evento_reserva)


@bp.timer_trigger(
    schedule="0 0 0 * * *",
    arg_name="timer",
    run_on_startup=False,
    use_monitor=False,
)
def espacios_actividades_bronze_a_silver(timer: func.TimerRequest):
    """Corre una vez por dia y arma las tres tablas de silver de Espacios
    (actividades, espacios afectados y reservas), cada una con su
    checkpoint. Cada parte va en su propio try: si una falla, las otras
    igual se actualizan y el error queda en el log."""
    for nombre, paso in (
        ("actividades", procesar_actividades),
        ("espacios afectados", procesar_afectados),
        ("reservas", procesar_reservas),
    ):
        try:
            paso()
        except Exception:
            logging.exception(f"Fallo el procesamiento de {nombre} de espacios bronze->silver")


def procesar_actividades() -> None:
    """Lee de bronze/Espacios Publicos y Cultura/ solo los blobs subidos
    despues del checkpoint de actividades (blobs.last_modified, que pone el
    propio storage) y aplica los que sean ActividadCreada/ActividadModificada."""
    checkpoint = leer_checkpoint_actividades()
    logging.info(f"Corrida diaria de espacios (actividades) bronze->silver. Checkpoint anterior: {checkpoint}")

    bronze_container = get_bronze_container_client()
    blobs_nuevos = [
        blob for blob in bronze_container.list_blobs(name_starts_with=PREFIJO_ESPACIOS)
        if checkpoint is None or blob.last_modified > checkpoint
    ]
    blobs_nuevos.sort(key=lambda blob: blob.last_modified)

    if not blobs_nuevos:
        logging.info("No hay actividades nuevas ni modificadas desde la ultima corrida.")
        return

    blob_client = get_blob_client_actividades()
    df = leer_tabla_actividades(blob_client)

    procesados = 0
    checkpoint_nuevo = checkpoint
    for blob in blobs_nuevos:
        contenido = bronze_container.download_blob(blob.name).readall()

        try:
            evento = json.loads(contenido)
        except json.JSONDecodeError:
            logging.warning(f"Blob no es JSON valido, se ignora: {blob.name}")
            continue

        df_actualizado = aplicar_evento_actividad(df, evento)
        if df_actualizado is not None:
            df = df_actualizado
            procesados += 1

        if checkpoint_nuevo is None or blob.last_modified > checkpoint_nuevo:
            checkpoint_nuevo = blob.last_modified

    escribir_tabla(blob_client, df)
    escribir_checkpoint_actividades(checkpoint_nuevo)
    logging.info(
        f"Procesados {procesados}/{len(blobs_nuevos)} eventos de actividad nuevos. "
        f"Tabla con {len(df)} filas. Nuevo checkpoint: {checkpoint_nuevo}"
    )
