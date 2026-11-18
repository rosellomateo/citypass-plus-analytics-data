# citypass-plus-analytics-data

Azure Function App que consume los eventos del bus de CityPass+ (el Kafka del Grupo 1 / EDA) y los persiste crudos (sin transformar) en la capa **bronze** de un Azure Storage Account, organizados por dominio de negocio y tipo de evento.

## ¿Qué hace?

**Vía principal — Kafka** ([bronze/bp_kafka_ingestion_bronze.py](bronze/bp_kafka_ingestion_bronze.py)): un timer trigger (`ingesta_kafka_bronze`) que corre cada 1 hora (minuto 50, para que la corrida de las 23:50 UTC entre antes del silver de las 00:00) y:

1. Se conecta al Kafka del bus con `SASL` + `OAUTHBEARER` (el token lo pide y renueva solo la librería contra el servicio de identidad del Grupo 2, con el cliente `grupo8`).
2. Lee lo nuevo de los tópicos que nos interesan (uno por event type, ej. `com.citypass.reclamos.ReclamoCreado`) con el consumer group `com.citypass.analitica.bronze`.
3. Deserializa cada mensaje Avro resolviendo el schema contra el gateway (`/api/v1/schemas/ids/{id}`).
4. Lo guarda como JSON (envelope `metadata` + `data`, igual que antes) en el container `bronze`.
5. Recién ahí commitea el offset. Si algo falla antes, el evento se vuelve a leer en la próxima corrida.

Cada corrida lee hasta ponerse al día o hasta 3 minutos, lo que pase primero.

- **Duplicados:** la entrega es *at-least-once*, así que un evento puede llegar dos veces. El nombre del blob sale de `metadata.receivedAt` + `metadata.eventId`, así que el segundo cae en el mismo nombre y se descarta.
- **Mensajes ilegibles:** si un mensaje no se puede deserializar se guarda tal cual en `bronze/_no_deserializables/<tópico>/<partición>_<offset>.avro` y se sigue, para no trabar la partición.
- **Tópicos:** por defecto los 10 de [`TOPICOS_DEFAULT`](bronze/bp_kafka_ingestion_bronze.py). Se pueden pisar con la variable `KAFKA_TOPICS` (separados por coma). Un tópico que todavía no existe en el bus solo genera un warning en el log.

**Vía manual — HTTP** ([bronze/bp_data_ingestion_bronze.py](bronze/bp_data_ingestion_bronze.py)): el endpoint `ingesta_eventos` sigue disponible para cargar eventos de prueba a mano. Recibe el JSON, lee `metadata.eventType` y lo guarda en la misma estructura de carpetas.

## Estructura de carpetas en el storage

El `eventType` sigue la convención `com.citypass.<dominio>.<TipoDeEvento>` (ej. `com.citypass.emergencias.AlertaEmergencia`). A partir de eso, el archivo se guarda en:

```
bronze/<Dominio>/<TipoDeEvento>/<TipoDeEvento>_<fechaHoraUTC>_<idCorto>.json
```

Por ejemplo, para `com.citypass.emergencias.AlertaEmergencia`:

```
bronze/Emergencias y Seguridad/AlertaEmergencia/AlertaEmergencia_20260905_143210_8b85a007.json
```

El 3er segmento del `eventType` (`emergencias`) se traduce a un nombre de carpeta "lindo" mediante el diccionario `DOMINIOS` en [bronze/bp_data_ingestion_bronze.py](bronze/bp_data_ingestion_bronze.py). Dominios ya mapeados:

| Clave (`eventType`) | Carpeta |
|---|---|
| `emergencias` | Emergencias y Seguridad |
| `reclamos` | Reclamos |
| `espacios` | Espacios Publicos y Cultura |
| `residuos` | Gestion de Residuos Inteligente |
| `movilidad` | Movilidad Urbana |

Si el dominio no está en el diccionario, se usa la clave cruda como nombre de carpeta. Si el evento no trae un `eventType` con el formato esperado, se guarda en `Otros/`.

Para sumar un dominio nuevo, agregar una entrada al diccionario `DOMINIOS`:

```python
DOMINIOS = {
    "emergencias": "Emergencias y Seguridad",
    "turismo": "Turismo y Cultura",
}
```

## Silver y gold: tablas y de dónde sale cada caso de uso

Silver corre todos los días a las 00:00 UTC (incremental, con un checkpoint por tabla) y gold a la 01:00 UTC
(recalcula todo y guarda un snapshot semanal los domingos). Para dispararlos a mano: `scripts/correr_pipeline_azure.ps1`.

| Dominio | Trigger silver | Tablas silver | Tablas gold |
|---|---|---|---|
| Reclamos | `reclamos_bronze_a_silver` | `reclamos.parquet` | `reclamos_resumen` |
| Emergencias | `emergencias_bronze_a_silver` | `emergencias.parquet` | `emergencias_resumen` |
| Espacios | `espacios_actividades_bronze_a_silver` | `actividades.parquet`, `espacios_afectados.parquet`, `reservas.parquet` | `actividades_resumen`, `espacios_afectados_resumen`, `reservas_resumen` |
| Residuos | `residuos_bronze_a_silver` | `intervenciones.parquet` | `intervenciones_resumen` |
| Movilidad | `movilidad_bronze_a_silver` (viajes **y** estaciones) | `viajes.parquet`, `estaciones_afectadas.parquet` | `movilidad_resumen` |
| LDAP | `autenticacion_bronze_a_silver` | `eventos_autenticacion.parquet` | `autenticacion_resumen` |

Las métricas de los 6 casos de uso (`MODELADO_EVENTOS_PARTE2.md`) salen de gold filtrando por estas columnas:

| Caso | Espacios | Residuos | Reclamos | Emergencias | Movilidad |
|---|---|---|---|---|---|
| 1 Evento climático | `actividades_resumen.cantidadAfectadasPorClima` (categoria CULTURAL) | `causaTipoAfectacion = EVENTO_CLIMATICO` | `causaTipo = EVENTO_CLIMATICO` | `causaTipo = EVENTO_CLIMATICO`, `criticidadAfectada` | `estacionesBloqueadasDistintas` (EVENTO_CLIMATICO) |
| 2 Reclamo | `espacios_afectados_resumen` con `causaTipo = RECLAMO` | `causaTipo = RECLAMO`, por `barrio` | `afectaOtrosModulos = true` | `causaTipo = RECLAMO` | `cantidadAfectaciones` (RECLAMO) |
| 3 Evento masivo | `actividades_resumen.cantidadMasivas` | `causaTipo = EVENTO_PUBLICO` | `causaTipo = EVENTO_PUBLICO` | `categoria = PREVENTIVA` | `cantidadPreventivas` (EVENTO_PUBLICO) |
| 4 Incidente vial | `espacios_afectados_resumen` con `causaTipo = INCIDENTE_VIAL` | `causaTipoAfectacion = INCIDENTE_VIAL` | `causaTipo = INCIDENTE_VIAL` | `causaTipo = INCIDENTE_VIAL` | `cantidadAfectaciones` (INCIDENTE_VIAL) |
| 5 Emergencia en bici | `reservas_resumen` con `afectada = true` | `origenProgramacion` EMERGENCIA vs. ALERTA | `vinculadoEmergenciaBici = true` vs. categoría BACHES/TRANSITO | `causaTipo = MOVILIDAD_BICICLETA`, `tiempoPromResolucion` | `cantidadViajesInterrumpidos` INCIDENTE_VIAL vs. NORMAL |
| 6 Reserva | `reservas_resumen` (`tipoReserva = ESPACIO`, CONFIRMADA) vs. total de actividades | `causaTipo = RESERVA` | `causaTipo = RESERVA` | `causaTipo = RESERVA` | `cantidadAfectaciones` (RESERVA) |

Notas:

- **Residuos tiene dos causas.** `causaTipo` es por qué se programó la intervención (viene en `IntervencionProgramada`) y
  `causaTipoAfectacion` es por qué quedó AFECTADA o CANCELADA (viene en `IntervencionActualizada`, también en el tópico `.v2`). Estados:
  PROGRAMADA, AFECTADA, CANCELADA y COMPLETADA (esta última llega como `IntervencionCompletada`).
- **Movilidad:** las estaciones afectadas se procesan dentro de `movilidad_bronze_a_silver`. El timer propio que tenían
  (`movilidad_estaciones_bronze_a_silver`) no se disparaba en Azure y silver de estaciones quedó congelado desde el 15/11.
- **Columnas que no se suman entre filas:** las que cuentan distintos (`estacionesAfectadasDistintas`, `espaciosDistintos`,
  `camionesDistintos`) se calculan por fila.
- **Reclamos cruza con otros módulos:** gold lee el silver de Residuos, Movilidad, Espacios y Emergencias. Si alguna de esas
  tablas no se puede leer, la columna correspondiente queda en `false` y el resumen se escribe igual.
- **Después de un cambio en silver de Residuos** hay que reprocesarlo completo (`scripts/reprocesar_residuos_silver.py`)
  para recalcular las intervenciones que ya estaban.

## Requisitos

- Python 3.11
- [Azure Functions Core Tools v4](https://learn.microsoft.com/azure/azure-functions/functions-run-local)
- Una cuenta de Azure Storage (o [Azurite](https://learn.microsoft.com/azure/storage/common/storage-use-azurite) para desarrollo local aislado)

## Configuración local

Crear `local.settings.json` (no se versiona) en la raíz del proyecto:

```json
{
  "IsEncrypted": false,
  "Values": {
    "AzureWebJobsStorage": "UseDevelopmentStorage=true",
    "FUNCTIONS_WORKER_RUNTIME": "python",
    "STORAGE_CONNECTION_STRING": "<connection string del storage account>",
    "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
    "KAFKA_SECURITY_PROTOCOL": "SASL_PLAINTEXT",
    "KAFKA_CLIENT_ID": "grupo8",
    "KAFKA_CLIENT_SECRET": "<secreto que nos pasó el Grupo 2>",
    "KAFKA_GROUP_ID": "com.citypass.analitica.bronze",
    "SCHEMA_REGISTRY_URL": "http://localhost:8080/api/v1"
  }
}
```

| Variable | Obligatoria | Default | Notas |
|---|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | sí | — | Broker del bus. Local: `localhost:9092` |
| `KAFKA_CLIENT_SECRET` | sí | — | Lo entrega el Grupo 2. **No commitearlo** |
| `SCHEMA_REGISTRY_URL` | sí | — | El gateway: `http://<host>:8080/api/v1` |
| `KAFKA_SECURITY_PROTOCOL` | no | `SASL_SSL` | `SASL_PLAINTEXT` contra el bus local |
| `KAFKA_CLIENT_ID` | no | `grupo8` | |
| `KAFKA_GROUP_ID` | no | `com.citypass.analitica.bronze` | Tiene que empezar con nuestro namespace |
| `KAFKA_TOKEN_URL` | no | `https://api.citypass-login.pp.ua/oauth/token` | |
| `KAFKA_TOPICS` | no | los de `TOPICOS_DEFAULT` | Lista separada por coma |

En Azure, las mismas variables van en *Configuration → Application settings* de la Function App.

## Correr localmente

```powershell
# 1. Crear el entorno virtual (una sola vez)
python -m venv .venv

# 2. Activar el entorno virtual
.\.venv\Scripts\Activate.ps1

# 3. Instalar dependencias
pip install -r requirements.txt

# 4. Levantar la function app
func start
```

Queda escuchando en `http://localhost:7071/api/ingesta_eventos`.

## Probar el endpoint

```powershell
curl -X POST http://localhost:7071/api/ingesta_eventos `
  -H "Content-Type: application/json" `
  -d '{
    "data": { "alertaId": "alerta-559", "tipo": "INCENDIO" },
    "metadata": { "eventType": "com.citypass.emergencias.AlertaEmergencia" }
  }'
```

Respuesta esperada:

```json
{"status": "ok", "archivo": "Emergencias y Seguridad/AlertaEmergencia/AlertaEmergencia_20260905_143210_8b85a007.json"}
```
