import azure.functions as func
import logging
import json
import os
import uuid
from datetime import datetime, timezone
from azure.storage.blob import BlobServiceClient

app = func.FunctionApp()

CONTAINER_NAME = "bronze"

@app.route(route="ingesta_eventos", auth_level=func.AuthLevel.FUNCTION)
def ingesta_eventos(req: func.HttpRequest) -> func.HttpResponse:
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

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        evento_id = str(uuid.uuid4())[:8]
        nombre_archivo = f"evento_{timestamp}_{evento_id}.json"

        contenido = json.dumps(evento, ensure_ascii=False)
        container_client.upload_blob(name=nombre_archivo, data=contenido, overwrite=False)

        logging.info(f"Evento guardado como {nombre_archivo}")

        return func.HttpResponse(
            json.dumps({"status": "ok", "archivo": nombre_archivo}),
            status_code=201,
            mimetype="application/json"
        )

    except Exception as e:
        logging.error(f"Error al guardar el evento: {str(e)}")
        return func.HttpResponse(
            "Error interno al guardar el evento",
            status_code=500
        )