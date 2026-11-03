# Eventos de autenticación para Analítica

## Objetivo

El backend de Login Federado publica hechos crudos de autenticación para que
el equipo de Analítica pueda construir indicadores, series temporales y
segmentaciones. Login no calcula métricas agregadas ni expone un endpoint de
reportes: produce un evento por cada operación relevante y el consumidor decide
cómo almacenarlo y agregarlo.

Actualmente se publican tres eventos:

| Evento interno | Tipo público en EDA | Significado |
| --- | --- | --- |
| `identidad.login` | `com.citypass.auth.LoginExitoso` | Una persona inició sesión correctamente. |
| `identidad.refresh` | `com.citypass.auth.TokenRenovado` | Un refresh token válido fue rotado y se emitieron tokens nuevos. |
| `identidad.logout` | `com.citypass.auth.SesionCerrada` | Se revocó una sesión o el conjunto de sesiones de una persona. |

Las definiciones de los campos se encuentran en:

- [`LoginExitoso.json`](./LoginExitoso.json)
- [`TokenRenovado.json`](./TokenRenovado.json)
- [`SesionCerrada.json`](./SesionCerrada.json)

## Flujo de entrega

```text
Login Federado -> Event Gateway -> consumidor de Analítica
```

Login publica cada evento en el Event Gateway mediante un `POST` a:

```text
/api/v1/event-types/{tipo-publico}/events
```

Por ejemplo:

```text
/api/v1/event-types/com.citypass.auth.LoginExitoso/events
```

El cuerpo enviado por Login contiene solamente los datos de negocio. El Event
Gateway es responsable de agregar los metadatos propios de la plataforma.

La entrega HTTP debe habilitarse en el despliegue con:

```dotenv
EDA_PUBLISHER=http
EDA_GATEWAY_URL=https://direccion-del-event-gateway
```

El valor predeterminado de `EDA_PUBLISHER` es `logging`. En ese modo los eventos
se escriben en el log de la aplicación para desarrollo y **no se entregan al
Event Gateway**.

## Campos comunes

Los tres eventos utilizan la misma estructura base:

| Campo | Tipo | Descripción |
| --- | --- | --- |
| `authEventId` | `string` | UUID único generado por Login. Debe usarse para deduplicar. |
| `actorSub` | `string` | Identificador estable de la persona autenticada. Es preferible a `username` para contar usuarios. |
| `occurredAt` | `long` | Instante del evento en Unix epoch milliseconds, referido a UTC. |
| `username` | `string \| null` | Nombre de usuario cuando está disponible. |
| `department` | `string \| null` | Módulo o departamento de la persona, por ejemplo `reclamos`. |
| `clientId` | `string \| null` | Aplicación cliente que inició la operación. |
| `chainId` | `string \| null` | UUID que relaciona las rotaciones de una misma cadena de refresh tokens. |
| `ipAddress` | `string \| null` | IP informada para la solicitud. |
| `userAgent` | `string \| null` | User-Agent informado por el cliente. |

Nunca se incluyen contraseñas, access tokens, refresh tokens ni hashes de
tokens.

### Tratamiento del tiempo

`occurredAt` representa un instante UTC. Para agrupar por día u hora de
Argentina, el consumidor debe convertirlo primero a la zona
`America/Argentina/Buenos_Aires`. Esto evita asignar al día incorrecto los
eventos cercanos a la medianoche local.

El contrato HTTP utiliza epoch milliseconds. El log de desarrollo puede
mostrar el instante en formato ISO-8601 dentro de un envelope de diagnóstico;
ese formato de log no debe tomarse como el contrato de consumo.

## 1. `LoginExitoso`

### Cuándo se genera

Se publica después de que:

1. las credenciales LDAP fueron validadas;
2. el control de intentos no bloqueó la operación;
3. el servicio de detección de anomalías permitió el acceso; y
4. Login emitió el access token y el refresh token inicial.

Un login rechazado no genera este evento.

### Particularidades

- `username`, `department`, `clientId`, `ipAddress` y `userAgent` normalmente
  tienen valor.
- `chainId` es `null`, aunque el login haya creado internamente la cadena de
  refresh. Por lo tanto, el contrato actual no permite vincular directamente
  el login inicial con sus refresh posteriores mediante `chainId`.

### Ejemplo

```json
{
  "authEventId": "75e794e7-29a3-4dc6-8662-cfb8567f7302",
  "actorSub": "U000042",
  "occurredAt": 1793362500000,
  "username": "nicolas",
  "department": "reclamos",
  "clientId": "citypass-reclamos-web",
  "chainId": null,
  "ipAddress": "192.168.1.10",
  "userAgent": "Mozilla/5.0"
}
```

### Usos posibles

- cantidad de logins exitosos por día, hora, semana o mes;
- usuarios únicos por período mediante `COUNT(DISTINCT actorSub)`;
- distribución por módulo mediante `department`;
- distribución por aplicación mediante `clientId`;
- horarios y días de mayor actividad;
- identificación de cambios de IP o User-Agent, con los controles de
  privacidad correspondientes.

Este evento por sí solo no permite calcular la tasa de éxito del login. Para
eso se necesitaría también la cantidad de intentos fallidos, que actualmente no
se publica por EDA.

## 2. `TokenRenovado`

### Cuándo se genera

Se publica cuando Login valida un refresh token, revalida a la persona contra
LDAP, rota el refresh token y emite un nuevo par de tokens.

Un refresh inválido, vencido, reutilizado o correspondiente a una persona que
ya no puede acceder no genera este evento.

### Particularidades

- `chainId` identifica la cadena de rotaciones y permite relacionar varios
  eventos `TokenRenovado` entre sí.
- Normalmente conserva `actorSub`, `username`, `department` y `clientId`.
- La IP y el User-Agent corresponden a la solicitud de renovación, por lo que
  pueden ser diferentes de los observados durante el login.

### Ejemplo

```json
{
  "authEventId": "208f6c5a-c64d-4662-9c99-550f64239485",
  "actorSub": "U000042",
  "occurredAt": 1793366100000,
  "username": "nicolas",
  "department": "reclamos",
  "clientId": "citypass-reclamos-web",
  "chainId": "cf32e735-7215-4469-b2aa-00b93321cd01",
  "ipAddress": "192.168.1.10",
  "userAgent": "Mozilla/5.0"
}
```

### Usos posibles

- volumen de renovaciones por período, módulo o aplicación;
- cantidad de cadenas de sesión activas que efectivamente realizan refresh;
- cantidad de renovaciones por cadena;
- duración observada entre el primer y el último refresh de una cadena;
- cambios de IP o User-Agent dentro de una misma cadena;
- relación entre volumen de logins y volumen de renovaciones.

La ausencia de renovaciones no demuestra por sí sola que una sesión haya
terminado: el usuario puede conservar un access token hasta su vencimiento o
simplemente dejar de utilizar la aplicación.

## 3. `SesionCerrada`

### Cuándo se genera

Se publica en dos situaciones:

1. **Logout individual:** se presenta un refresh token conocido y Login lo
   revoca.
2. **Revocación masiva:** Login revoca las sesiones de una persona por baja de
   cuenta, cambio de contraseña o recuperación de contraseña.

Un logout con un token desconocido no genera un evento.

### Particularidades

En un logout individual:

- `chainId`, `clientId`, `department`, IP y User-Agent normalmente tienen valor;
- `username` es `null`.

En una revocación masiva:

- se publica **un solo evento por persona** si se revocó al menos una sesión;
- no se publica un evento por cada token revocado;
- `clientId`, `chainId`, `ipAddress`, `userAgent` y `username` son `null`;
- `department` identifica el módulo de la persona.

### Ejemplo de logout individual

```json
{
  "authEventId": "e997571a-c1b7-481d-bcca-155903b40830",
  "actorSub": "U000042",
  "occurredAt": 1793369700000,
  "username": null,
  "department": "reclamos",
  "clientId": "citypass-reclamos-web",
  "chainId": "cf32e735-7215-4469-b2aa-00b93321cd01",
  "ipAddress": "192.168.1.10",
  "userAgent": "Mozilla/5.0"
}
```

### Ejemplo de revocación masiva

```json
{
  "authEventId": "0e821a99-c578-4526-839b-cb61435feab8",
  "actorSub": "U000042",
  "occurredAt": 1793370000000,
  "username": null,
  "department": "reclamos",
  "clientId": null,
  "chainId": null,
  "ipAddress": null,
  "userAgent": null
}
```

### Usos posibles

- cantidad de cierres de sesión explícitos por período;
- cierres por módulo o aplicación;
- correlación del logout individual con los refresh de la misma `chainId`;
- cantidad de personas afectadas por revocaciones masivas;
- comparación entre sesiones que realizan logout explícito y sesiones que
  dejan de producir actividad.

No debe interpretarse cada `SesionCerrada` como una única sesión revocada: el
evento de revocación masiva puede representar varias sesiones. Tampoco existe
un evento cuando un access token o refresh token vence naturalmente.

## Recomendaciones para el consumidor

### 1. Deduplicar

Usar `authEventId` como clave única. Si el mecanismo de entrega reintenta una
publicación, el mismo hecho no debe contarse dos veces.

### 2. Conservar el tipo público

Al persistir el evento, guardar también su tipo público completo. Esto permite
mantener una tabla común para los tres eventos y distinguirlos sin inferir el
tipo a partir de campos nulos.

Una estructura analítica posible es:

| Columna | Origen |
| --- | --- |
| `event_type` | Tipo público EDA. |
| `auth_event_id` | `authEventId`. |
| `event_time_utc` | Conversión de `occurredAt`. |
| `actor_sub` | `actorSub`. |
| `username` | `username`. |
| `department` | `department`. |
| `client_id` | `clientId`. |
| `chain_id` | `chainId`. |
| `ip_address` | `ipAddress`. |
| `user_agent` | `userAgent`. |
| `ingested_at` | Momento en que Analítica recibió el evento. |

`ingested_at` permite medir retrasos de entrega comparándolo con
`event_time_utc`.

### 3. Crear dimensiones temporales locales

Ejemplo conceptual para PostgreSQL, suponiendo una tabla
`authentication_events`:

```sql
SELECT
    date_trunc(
        'hour',
        to_timestamp(occurred_at / 1000.0)
            AT TIME ZONE 'America/Argentina/Buenos_Aires'
    ) AS hora_local,
    department,
    count(*) AS logins_exitosos,
    count(DISTINCT actor_sub) AS usuarios_unicos
FROM authentication_events
WHERE event_type = 'com.citypass.auth.LoginExitoso'
GROUP BY 1, 2
ORDER BY 1, 2;
```

El mismo criterio puede utilizarse para generar `fecha_local`, `hora_local`,
`dia_de_semana`, `semana` y `mes`.

### 4. Tratar los datos como sensibles

`actorSub`, `username`, IP y User-Agent pueden identificar o perfilar a una
persona. El consumidor debe aplicar controles de acceso, retención limitada y
minimización de datos. Los tableros generales deberían utilizar valores
agregados y evitar exponer identificadores individuales.

## Indicadores que pueden construirse

| Indicador | Cálculo sugerido | Consideración |
| --- | --- | --- |
| Logins exitosos | Cantidad de `LoginExitoso`. | No representa intentos totales. |
| Usuarios activos por login | `COUNT(DISTINCT actorSub)` de `LoginExitoso`. | Define actividad como inicio de sesión. |
| Logins por módulo | `LoginExitoso` agrupado por `department`. | Puede haber valores nulos en datos incompletos. |
| Logins por aplicación | `LoginExitoso` agrupado por `clientId`. | El cliente administrador puede ser transversal. |
| Renovaciones | Cantidad de `TokenRenovado`. | Mide rotaciones, no usuarios. |
| Cadenas con actividad | `COUNT(DISTINCT chainId)` de `TokenRenovado`. | No incluye cadenas que nunca hicieron refresh. |
| Cierres explícitos | `SesionCerrada` con `chainId IS NOT NULL`. | No incluye vencimientos naturales. |
| Personas con revocación masiva | `SesionCerrada` con `chainId IS NULL`. | Un evento puede representar varias sesiones. |

## Limitaciones actuales

- No se publican intentos de login fallidos; por eso no puede calcularse la
  tasa de éxito o rechazo solamente con estos eventos.
- No existe un evento por vencimiento natural de token.
- `LoginExitoso` no expone el `chainId` de la cadena creada durante ese login.
- `SesionCerrada` no incluye un campo que diferencie explícitamente logout,
  baja de cuenta, cambio de contraseña y recuperación de contraseña. En el
  contrato actual solo puede distinguirse, de manera aproximada, una
  revocación masiva por la ausencia de `chainId` y `clientId`.
- Los eventos describen actividad observada, pero no constituyen por sí solos
  un inventario exacto de sesiones activas en un instante determinado.

