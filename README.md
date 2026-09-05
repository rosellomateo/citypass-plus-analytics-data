# citypass-plus-analytics-data

Azure Function App que recibe eventos del gateway de CityPass+ y los persiste crudos (sin transformar) en la capa **bronze** de un Azure Storage Account, organizados por dominio de negocio y tipo de evento.

## ¿Qué hace?

Expone un único endpoint HTTP (`ingesta_eventos`) que:

1. Recibe un evento en formato JSON.
2. Lee `metadata.eventType` para determinar dónde guardarlo.
3. Sube el JSON tal cual llegó como un blob en el container `bronze`.

## Estructura de carpetas en el storage

El `eventType` sigue la convención `com.citypass.<dominio>.<TipoDeEvento>` (ej. `com.citypass.emergencias.AlertaEmergencia`). A partir de eso, el archivo se guarda en:

```
bronze/<Dominio>/<TipoDeEvento>/<TipoDeEvento>_<fechaHoraUTC>_<idCorto>.json
```

Por ejemplo, para `com.citypass.emergencias.AlertaEmergencia`:

```
bronze/Emergencias y Seguridad/AlertaEmergencia/AlertaEmergencia_20260905_143210_8b85a007.json
```

El 3er segmento del `eventType` (`emergencias`) se traduce a un nombre de carpeta "lindo" mediante el diccionario `DOMINIOS` en [function_app.py](function_app.py). Dominios ya mapeados:

| Clave (`eventType`) | Carpeta |
|---|---|
| `emergencias` | Emergencias y Seguridad |
| `reclamos` | Reclamos |
| `espacios` | Espacios Publicos y Cultura |
| `residuos` | Gestion de residuos inteligente |
| `movilidad` | Movilidad Urbana |

Si el dominio no está en el diccionario, se usa la clave cruda como nombre de carpeta. Si el evento no trae un `eventType` con el formato esperado, se guarda en `Otros/`.

Para sumar un dominio nuevo, agregar una entrada al diccionario `DOMINIOS`:

```python
DOMINIOS = {
    "emergencias": "Emergencias y Seguridad",
    "turismo": "Turismo y Cultura",
}
```

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
    "STORAGE_CONNECTION_STRING": "<connection string del storage account>"
  }
}
```

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
