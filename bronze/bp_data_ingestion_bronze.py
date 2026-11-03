import azure.functions as func
import logging
import json
import os
import re
import uuid
from datetime import datetime, timezone
from azure.core.exceptions import ResourceExistsError
from azure.storage.blob import BlobServiceClient

bp = func.Blueprint()

CONTAINER_NAME = "bronze"

# Mapea el dominio de negocio (tercer segmento del eventType, ej. "emergencias"
# en "com.citypass.emergencias.AlertaEmergencia") al nombre de carpeta en el storage.
# Agregar acá una entrada por cada dominio nuevo.
DOMINIOS = {
    "emergencias": "Emergencias y Seguridad",
    "reclamos": "Reclamos",
    "espacios": "Espacios Publicos y Cultura",
    "residuos": "Gestion de Residuos Inteligente",
    "movilidad": "Movilidad Urbana",
    "auth": "Autenticacion"
}

CARPETA_DEFAULT = "Otros"


def resolver_carpeta_y_tipo(event_type: str) -> tuple[str, str]:
    """A partir del eventType (ej. com.citypass.emergencias.AlertaEmergencia)
    devuelve ('Dominio/Subcarpeta', tipo_evento), usando el 3er segmento como
    dominio (traducido via DOMINIOS) y el último segmento como subcarpeta y
    tipo de evento."""
    partes = event_type.split(".")
    # Las versiones mayores de un event type viven en otro tópico con sufijo
    # (com.citypass.movilidad.BiciDevuelta.v2): el tipo sigue siendo BiciDevuelta.
    if len(partes) > 4 and re.fullmatch(r"v\d+", partes[-1]):
        partes = partes[:-1]
    if len(partes) < 4:
        return CARPETA_DEFAULT, "evento"

    dominio_key = partes[2]
    tipo_evento = partes[-1]
    dominio = DOMINIOS.get(dominio_key, dominio_key)
    return f"{dominio}/{tipo_evento}", tipo_evento


def armar_nombre_blob(evento, event_type_default: str = "") -> str:
    """Nombre del blob en bronze: <carpeta>/<tipo>_<fechaHoraUTC>_<idCorto>.json.

    Si el evento trae metadata del bus (receivedAt + eventId), el nombre sale
    de ahí y es siempre el mismo para el mismo evento: así un evento que Kafka
    entrega dos veces (at-least-once) cae en el mismo blob y no se duplica.
    Si no la trae (ej. un evento de prueba posteado a mano), se usa la hora
    actual y un id al azar, como antes."""
    metadata = evento.get("metadata", {}) if isinstance(evento, dict) else {}
    if not isinstance(metadata, dict):
        metadata = {}
    event_type = metadata.get("eventType") or event_type_default
    carpeta, tipo_evento = resolver_carpeta_y_tipo(event_type)

    received_at = metadata.get("receivedAt")
    event_id = metadata.get("eventId")
    if isinstance(received_at, (int, float)) and event_id:
        momento = datetime.fromtimestamp(received_at / 1000, tz=timezone.utc)
        id_corto = str(event_id)[:8]
    else:
        momento = datetime.now(timezone.utc)
        id_corto = str(uuid.uuid4())[:8]

    timestamp = momento.strftime("%Y%m%d_%H%M%S")
    return f"{carpeta}/{tipo_evento}_{timestamp}_{id_corto}.json"


def guardar_evento_en_bronze(container_client, evento, event_type_default: str = "",
                             serializar_default=None) -> tuple[str, bool]:
    """Sube el evento crudo al container bronze. Devuelve (nombre_blob, es_nuevo):
    es_nuevo=False si ya existía (evento duplicado), en cuyo caso no se pisa."""
    nombre_archivo = armar_nombre_blob(evento, event_type_default)
    contenido = json.dumps(evento, ensure_ascii=False, default=serializar_default)
    try:
        container_client.upload_blob(name=nombre_archivo, data=contenido, overwrite=False)
    except ResourceExistsError:
        logging.info(f"Evento duplicado, ya estaba guardado como {nombre_archivo}")
        return nombre_archivo, False

    logging.info(f"Evento guardado como {nombre_archivo}")
    return nombre_archivo, True


@bp.route(route="ingesta_eventos", auth_level=func.AuthLevel.FUNCTION)
def ingesta_eventos(req: func.HttpRequest) -> func.HttpResponse:
    """Ingesta manual por HTTP. La vía normal es el consumidor de Kafka
    (bp_kafka_ingestion_bronze.py); este endpoint queda para cargar eventos de
    prueba a mano."""
    logging.info("Recibida una solicitud de ingesta de evento")

    # 1. Parsear el body como JSON
    try:
        evento = req.get_json()
    except ValueError:
        return func.HttpResponse(
            "El body debe ser un JSON válido",
            status_code=400
        )

    # 2. Leer la connection string desde variables de entorno
    connection_string = os.environ.get("STORAGE_CONNECTION_STRING")
    if not connection_string:
        logging.error("Falta la variable de entorno STORAGE_CONNECTION_STRING")
        return func.HttpResponse(
            "Error de configuración del servidor",
            status_code=500
        )

    # 3. Guardar el evento crudo en el container bronze
    try:
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_service_client.get_container_client(CONTAINER_NAME)

        nombre_archivo, es_nuevo = guardar_evento_en_bronze(container_client, evento)

        return func.HttpResponse(
            json.dumps({"status": "ok", "archivo": nombre_archivo, "duplicado": not es_nuevo}),
            status_code=201 if es_nuevo else 200,
            mimetype="application/json"
        )

    except Exception as e:
        logging.error(f"Error al guardar el evento: {str(e)}")
        return func.HttpResponse(
            "Error interno al guardar el evento",
            status_code=500
        )
