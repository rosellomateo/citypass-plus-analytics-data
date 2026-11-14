"""Ingesta a bronze desde el bus de eventos (Kafka del Grupo 1 / EDA).

Timer trigger cada 1 hora que se conecta al Kafka del bus, lee lo nuevo de
los tópicos que nos interesan y guarda cada evento crudo en bronze, igual que
lo hacía el endpoint HTTP (mismo envelope metadata + data, mismas carpetas).
Las capas silver no se enteran del cambio.

Cómo funciona la conexión (ver README del bus, sección 6):
- Autenticación SASL/OAUTHBEARER: librdkafka pide y renueva solo el token
  contra el servicio de identidad del Grupo 2 con nuestro client_id/secret.
- Los mensajes vienen en Avro formato Confluent; el schema se resuelve contra
  el gateway, que responde /schemas/ids/{id} igual que un Schema Registry.
- El group.id tiene que empezar con nuestro namespace (com.citypass.analitica).

El orden es leer -> guardar en bronze -> commitear el offset, nunca al revés:
si algo falla antes del commit el evento se vuelve a leer en la próxima
corrida. Los duplicados que eso puede generar no llegan a bronze porque el
nombre del blob sale del eventId (ver armar_nombre_blob).
"""
import azure.functions as func
import base64
import logging
import os
import time
import uuid
from datetime import date, datetime, timezone
from decimal import Decimal

from azure.storage.blob import BlobServiceClient
from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

from bronze.bp_data_ingestion_bronze import CONTAINER_NAME, guardar_evento_en_bronze

bp = func.Blueprint()

# --- Configuracion ---------------------------------------------------------

# Cada hora, en el minuto 50: así la corrida de las 23:50 UTC termina antes de
# que arranquen las capas silver (00:00 UTC) y entra en el procesamiento del día.
SCHEDULE = "0 50 * * * *"

# Tópicos (= fqn del event type) que consumimos. Se pueden pisar con la
# variable KAFKA_TOPICS (separados por coma) sin tocar código.
TOPICOS_DEFAULT = [
    # Los que están registrados en el bus de test (GET /api/v1/event-types).
    # Ojo: no son los que habíamos supuesto en EVENTOS_POR_GRUPO.md.
    "com.citypass.reclamos.ReclamoCreado",
    # Rompio compatibilidad al agregar causaId/causaTipo/geolocalizacion (9/nov):
    # el gateway abrio un topico nuevo con sufijo .v2 y dejo el de arriba sirviendo
    # su historial viejo. Hay que seguir escuchando los dos: el viejo por si quedan
    # eventos sin leer, el nuevo porque ahi es donde va a publicar Reclamos de aca
    # en adelante.
    "com.citypass.reclamos.ReclamoCreado.v2",
    # Nueva rotura de compatibilidad (14/nov): se agrego el campo "estado"
    # (obligatorio, sin default) -- abrio .v3 y dejo v1/v2 sirviendo su
    # historial. bp_reclamos_silver.py no necesito cambios: _tipo_evento()
    # ya le saca el sufijo .v\d+ de forma generica, y "estado" es redundante
    # con el estado_actual que silver calcula solo (mismo caso que
    # EmergenciaCreada.estado).
    "com.citypass.reclamos.ReclamoCreado.v3",
    "com.citypass.reclamos.ReclamoActualizado",
    "com.citypass.reclamos.ReclamoResuelto",
    "com.citypass.reclamos.ReclamoPostEventoClimatico",
    # EmergenciaReportada (el que escuchabamos antes) era un evento viejo con
    # otra forma (tipo/prioridad/descripcion/direccion); el ciclo de estados
    # que arma bp_emergencias_silver.py corresponde a estos dos:
    "com.citypass.emergencias.EmergenciaCreada",
    "com.citypass.emergencias.EmergenciaActualizada",
    "com.citypass.movilidad.EmergenciaBicicleta",
    "com.citypass.movilidad.ReporteBicicleta",
    "com.citypass.movilidad.EstacionUrbanaAfectada",
    "com.citypass.movilidad.EstacionUrbanaRestaurada",
    # Propuesto por nosotros (ver CONTRATO_MOVILIDAD.md #2.3), todavia no
    # confirmado por Movilidad -- no esta en /event-types al 14/nov. Lo
    # suscribimos igual: si no existe el topico, Kafka solo tira un warning
    # (ver consumir_lote), no rompe la ingesta del resto.
    "com.citypass.movilidad.ViajeFinalizado",
    "com.citypass.residuos.Crear_Evento",
    "com.citypass.residuos.CriticidadClimaticaConfirmada",
    "com.citypass.residuos.IntervencionProgramada",
    "com.citypass.residuos.IntervencionActualizada",
    "com.citypass.turismo.CreacionEvento",
    # Espacios Publicos: "actividades" (eventos/talleres con cupo).
    "com.citypass.espacios.ActividadCreada.v2",
    # Rotura de compatibilidad (14/nov): se agrego organizadorTipo
    # (obligatorio) -- abrio .v3 y dejo v2 sirviendo su historial. metadata.
    # eventType no lleva el sufijo de version (confirmado en los eventos
    # reales), asi que aplicar_evento_actividad() no necesita cambios.
    "com.citypass.espacios.ActividadCreada.v3",
    "com.citypass.espacios.ActividadModificada",
    "com.citypass.espacios.CriticidadClimaticaConfirmada",
    # Grupo 9 (LDAP / Login Federado) -- ver Eventos-login.md.
    "com.citypass.auth.LoginExitoso",
    "com.citypass.auth.TokenRenovado",
    "com.citypass.auth.SesionCerrada",
]

NAMESPACE = "com.citypass.analitica"
CLIENT_ID_DEFAULT = "grupo8"
GROUP_ID_DEFAULT = f"{NAMESPACE}.bronze"
TOKEN_URL_DEFAULT = "https://api.citypass-login.pp.ua/oauth/token"

# Una corrida lee hasta ponerse al día o hasta agotar este tiempo (lo que pase
# primero), así nunca se acerca al timeout de 5 min del plan Consumption.
TIEMPO_MAXIMO_SEGUNDOS = 180
# Hasta que el broker nos asigna particiones (unirse al consumer group tarda
# unos segundos) los polls vacíos no significan "no hay nada nuevo".
ESPERA_MAXIMA_ASIGNACION_SEGUNDOS = 45
# Con particiones asignadas, esta cantidad de polls seguidos sin mensajes
# significa que ya leímos todo lo que había.
POLLS_VACIOS_PARA_CORTAR = 5
POLL_TIMEOUT_SEGUNDOS = 1.0

CARPETA_NO_DESERIALIZABLES = "_no_deserializables"


def leer_topicos() -> list[str]:
    valor = os.environ.get("KAFKA_TOPICS", "")
    topicos = [t.strip() for t in valor.split(",") if t.strip()]
    return topicos or TOPICOS_DEFAULT


def armar_config_consumer() -> dict:
    """Config de librdkafka a partir de las variables de entorno. Falla con
    KeyError si falta alguna obligatoria."""
    return {
        "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        "group.id": os.environ.get("KAFKA_GROUP_ID", GROUP_ID_DEFAULT),
        "auto.offset.reset": "earliest",
        # Sin esto la librería marca el offset al entregarnos el mensaje, antes
        # de guardarlo en bronze, y un corte en el medio pierde eventos.
        "enable.auto.commit": False,
        "security.protocol": os.environ.get("KAFKA_SECURITY_PROTOCOL", "SASL_SSL"),
        "sasl.mechanisms": "OAUTHBEARER",
        "sasl.oauthbearer.method": "oidc",
        "sasl.oauthbearer.client.id": os.environ.get("KAFKA_CLIENT_ID", CLIENT_ID_DEFAULT),
        "sasl.oauthbearer.client.secret": os.environ["KAFKA_CLIENT_SECRET"],
        "sasl.oauthbearer.token.endpoint.url": os.environ.get("KAFKA_TOKEN_URL", TOKEN_URL_DEFAULT),
    }


def valor_avro_a_json(valor):
    """`default` de json.dumps para los tipos que devuelve fastavro y JSON no
    conoce. Se convierten a lo mismo que manda el gateway en su JSON (ver
    README del bus, "Tipos disponibles"), así bronze queda igual que antes."""
    if isinstance(valor, datetime):
        # timestamp-millis -> epoch millis
        if valor.tzinfo is None:
            valor = valor.replace(tzinfo=timezone.utc)
        return int(valor.timestamp() * 1000)
    if isinstance(valor, date):
        return valor.isoformat()
    if isinstance(valor, Decimal):
        return float(valor)
    if isinstance(valor, (bytes, bytearray)):
        return base64.b64encode(valor).decode("ascii")
    if isinstance(valor, uuid.UUID):
        return str(valor)
    raise TypeError(f"Tipo no serializable a JSON: {type(valor).__name__}")


def _guardar_no_deserializable(container_client, msg):
    """Un mensaje que no se puede leer no se puede saltear sin más (se
    perdería) ni dejar sin commitear (trabaría la partición para siempre):
    se guarda tal cual en una carpeta aparte para revisarlo a mano."""
    nombre = f"{CARPETA_NO_DESERIALIZABLES}/{msg.topic()}/{msg.partition()}_{msg.offset()}.avro"
    container_client.upload_blob(name=nombre, data=msg.value() or b"", overwrite=True)
    return nombre


def consumir_lote(consumer, deserializar, container_client, reloj=time.monotonic) -> dict:
    """Lee mensajes hasta ponerse al día (o hasta TIEMPO_MAXIMO_SEGUNDOS), y
    por cada uno: deserializa -> guarda en bronze -> commitea. Si guardar en
    bronze falla, la excepción sube sin commitear: la próxima corrida retoma
    desde ese mensaje."""
    stats = {"guardados": 0, "duplicados": 0, "no_deserializables": 0}
    inicio = reloj()
    polls_vacios = 0

    while reloj() - inicio < TIEMPO_MAXIMO_SEGUNDOS:
        msg = consumer.poll(POLL_TIMEOUT_SEGUNDOS)

        if msg is None:
            if consumer.assignment():
                polls_vacios += 1
                if polls_vacios >= POLLS_VACIOS_PARA_CORTAR:
                    break
            elif reloj() - inicio >= ESPERA_MAXIMA_ASIGNACION_SEGUNDOS:
                logging.warning("Kafka no asignó ninguna partición; ¿existen los tópicos?")
                break
            continue
        polls_vacios = 0

        if msg.error():
            # Ej. un tópico de la lista que todavía no se creó en el bus.
            logging.warning(f"Error de Kafka en {msg.topic()}: {msg.error()}")
            continue

        try:
            # Sin SerializationContext: con uno, el AvroDeserializer (>=2.x) resuelve
            # primero un "subject" vía /dek-registry (asociaciones de Data Contracts),
            # algo que este gateway no implementa y devuelve 401 -- aunque el schema id
            # embebido en el mensaje sea perfectamente resoluble. El propio README del
            # bus (Grupo 1, sección 6) deserializa con `None`, igual que acá.
            evento = deserializar(msg.value(), None)
        except Exception as e:
            nombre = _guardar_no_deserializable(container_client, msg)
            logging.error(f"No se pudo deserializar {msg.topic()}[{msg.partition()}]@{msg.offset()}: {e}. Guardado en {nombre}")
            stats["no_deserializables"] += 1
            consumer.commit(message=msg, asynchronous=False)
            continue

        if evento is not None:
            _, es_nuevo = guardar_evento_en_bronze(
                container_client, evento,
                event_type_default=msg.topic(),
                serializar_default=valor_avro_a_json,
            )
            stats["guardados" if es_nuevo else "duplicados"] += 1

        # Después de guardar, nunca antes. `message=` commitea offset+1.
        consumer.commit(message=msg, asynchronous=False)

    return stats


@bp.timer_trigger(schedule=SCHEDULE, arg_name="timer", run_on_startup=False, use_monitor=True)
def ingesta_kafka_bronze(timer: func.TimerRequest) -> None:
    logging.info("Arrancando ingesta de Kafka a bronze")

    connection_string = os.environ.get("STORAGE_CONNECTION_STRING")
    schema_registry_url = os.environ.get("SCHEMA_REGISTRY_URL")
    if not connection_string or not schema_registry_url:
        logging.error("Faltan STORAGE_CONNECTION_STRING y/o SCHEMA_REGISTRY_URL")
        return

    try:
        config = armar_config_consumer()
    except KeyError as e:
        logging.error(f"Falta la variable de entorno {e.args[0]}")
        return

    container_client = BlobServiceClient.from_connection_string(connection_string) \
        .get_container_client(CONTAINER_NAME)
    deserializar = AvroDeserializer(SchemaRegistryClient({"url": schema_registry_url}))

    topicos = leer_topicos()
    consumer = Consumer(config)
    try:
        consumer.subscribe(topicos)
        stats = consumir_lote(consumer, deserializar, container_client)
    finally:
        # Con auto-commit apagado, close() no commitea nada que no hayamos guardado.
        consumer.close()

    logging.info(f"Ingesta de Kafka terminada ({len(topicos)} tópicos): {stats}")
