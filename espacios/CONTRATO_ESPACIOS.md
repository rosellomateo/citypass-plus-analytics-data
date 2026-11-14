# Contrato de eventos — Parte 2 · Espacios Públicos y Cultura

`namespace`: `com.citypass.espacios` · versión del contrato: 29/oct/2026

Para la Parte 2 vamos a armar un tablero con 30 métricas (6 casos de uso transversales × 5
módulos). Este documento dice **qué publica y consume Espacios**. Lo necesitan también los otros
4 equipos: Espacios **origina dos de las cinco causas** (alerta climática y evento público) y la
reserva ciudadana que dispara el caso 6.

## 1. Convenciones comunes (iguales para los 5 equipos)

- Todos los eventos viajan en el envelope del bus (`metadata` + `data`); acá solo se define `data`.
- Timestamps en ISO-8601 UTC. Geolocalización como `latitud` / `longitud` (double, WGS84).
- Todos los eventos nuevos llevan `correlationId`.

### 1.1 Causa: `causaId` / `causaTipo`

Par de campos que se agrega a varios eventos de todos los módulos para decir **qué originó** una
entidad (o su afectación).

| Campo | Tipo | Notas |
|---|---|---|
| `causaId` | string \| null | id de la entidad que lo originó. `null` = flujo normal, sin causa externa |
| `causaTipo` | enum \| null | a qué entidad pertenece `causaId`. `null` si y solo si `causaId` es `null` |

| `causaTipo` | `causaId` es… | Evento origen | Lo publica |
|---|---|---|---|
| `EVENTO_CLIMATICO` | `alertaClimaticaId` | `AlertaClimaticaGenerada` | **Espacios** |
| `RECLAMO` | `reclamoId` | `ReclamoCreado` | Reclamos |
| `INCIDENTE_VIAL` | `incidenteId` | `IncidenteVialCreado` | Movilidad |
| `EVENTO_PUBLICO` | `eventoId` | `EventoPublicoCreado` | **Espacios** |
| `RESERVA` | `reservaId` | `ReservaCreada` (`tipoReserva = ESPACIO`) | **Espacios** |

Cada equipo decide **con su propia regla de negocio** cuándo completar la causa. Analítica no
infiere causas: solo cuenta lo que viene en el evento.

## 2. Qué publica Espacios

### 2.1 `AlertaClimaticaGenerada` — nuevo (origen del caso 1)

| Campo | Formato | Notas |
|---|---|---|
| `alertaClimaticaId` | uuid | |
| `tipoEvento` | enum | según la fuente climática. Propuesta: `TORMENTA`, `LLUVIA_INTENSA`, `VIENTO_FUERTE`, `CALOR_EXTREMO`, `GRANIZO`, `INUNDACION` |
| `severidad` | enum | `ALTA`, `MEDIA`, `BAJA` |
| `latitud` / `longitud` | double | centro del área afectada |
| `radioAfectacionKm` | double \| null | `null` = toda la ciudad |
| `descripcion` | string | |
| `vigenteDesde` | datetime | |
| `vigenteHastaEstimado` | datetime \| null | ventana para decidir si algo es "a causa" de la alerta |
| `correlationId` | string | |
| `occurredAt` | datetime | |

```json
{
  "alertaClimaticaId": "2b6f1d8a-0c34-4e4e-8f0a-6a4c9b1e7d22",
  "tipoEvento": "TORMENTA",
  "severidad": "ALTA",
  "latitud": -34.6037,
  "longitud": -58.3816,
  "radioAfectacionKm": 12.5,
  "descripcion": "Tormenta fuerte con ráfagas de hasta 80 km/h",
  "vigenteDesde": "2026-11-03T15:00:00.000Z",
  "vigenteHastaEstimado": "2026-11-04T03:00:00.000Z",
  "correlationId": "c-clima-0012",
  "occurredAt": "2026-11-03T14:30:00.000Z"
}
```

### 2.2 `EventoPublicoCreado` — nuevo (origen del caso 3)

Hoy solo se publican las inscripciones (`ReservaCreada` con `tipoReserva = EVENTO`). Este evento
anuncia el evento en sí, para que los demás módulos puedan planificar.

| Campo | Formato | Notas |
|---|---|---|
| `eventoId` | uuid | mismo id que `ReservaCreada.recursoId` de las inscripciones |
| `espacioId` | string | |
| `nombre` | string | |
| `categoria` | enum | `CULTURAL`, `DEPORTIVO`, `RECREATIVO` |
| `latitud` / `longitud` | double | |
| `fechaInicio` / `fechaFin` | datetime | |
| `cupoMaximo` | int \| null | |
| `masivo` | boolean | `true` si `cupoMaximo ≥ 1000` (umbral único para todos los módulos) |
| `organizadorId` | string | |
| `correlationId` | string | |
| `createdAt` | datetime | |

```json
{
  "eventoId": "f2d8a6b1-4c7e-4b9a-a3d5-8e1f0c6b2a99",
  "espacioId": "ESP-PLAZA-012",
  "nombre": "Festival de Jazz en el Rosedal",
  "categoria": "CULTURAL",
  "latitud": -34.5711,
  "longitud": -58.4180,
  "fechaInicio": "2026-11-15T18:00:00.000Z",
  "fechaFin": "2026-11-15T23:59:00.000Z",
  "cupoMaximo": 5000,
  "masivo": true,
  "organizadorId": "org-cultura-caba",
  "correlationId": "c-5520",
  "createdAt": "2026-11-01T10:00:00.000Z"
}
```

### 2.3 `EventoPublicoActualizado` — nuevo

| Campo | Formato | Notas |
|---|---|---|
| `eventoId` | uuid | |
| `estadoAnterior` / `estadoNuevo` | enum | `PROGRAMADO` → `EN_CURSO` → `FINALIZADO` (+ `CANCELADO` desde `PROGRAMADO`/`EN_CURSO`) |
| `motivo` | string \| null | |
| `causaId` / `causaTipo` | string \| null / enum \| null | por qué se canceló (ej. `EVENTO_CLIMATICO`) |
| `correlationId` | string | |
| `updatedAt` | datetime | |

### 2.4 `EspacioPublicoAfectado` / `EspacioPublicoRestaurado` — nuevos

**`EspacioPublicoAfectado`**

| Campo | Formato | Notas |
|---|---|---|
| `afectacionId` | uuid | |
| `espacioId` | string | |
| `categoria` | enum | `PLAZA`, `SALON`, `CANCHA`, `AUDITORIO` |
| `latitud` / `longitud` | double | |
| `tipoAfectacion` | enum | `CERRADO`, `USO_PARCIAL`, `ACCESO_RESTRINGIDO` |
| `afectacionDesde` | datetime | |
| `afectacionHastaEstimada` | datetime \| null | |
| `causaId` / `causaTipo` | string / enum | valores usados: `RECLAMO`, `INCIDENTE_VIAL` |
| `correlationId` | string | |
| `occurredAt` | datetime | |

**`EspacioPublicoRestaurado`**: `afectacionId` (uuid), `espacioId` (string), `correlationId`
(string), `restauradoAt` (datetime).

### 2.5 `ReservaCreada` — cambio en evento existente

| Campo | Cambio |
|---|---|
| `zona` | **se elimina** |
| `latitud` / `longitud` | **nuevo** (double) — ubicación del espacio o del evento |

El resto de los campos sigue igual. Para el caso 6, los demás módulos usan las reservas con
`tipoReserva = ESPACIO` (un ciudadano reserva un espacio) y su `reservaId` como `causaId`.

`ReservaActualizada`: **sin cambios**.

## 3. Qué consume Espacios (para completar la causa)

| Evento | Namespace | Para completar |
|---|---|---|
| `ReclamoCreado` | `com.citypass.reclamos` | `RECLAMO` en `EspacioPublicoAfectado` |
| `IncidenteVialCreado` / `IncidenteVialActualizado` | `com.citypass.movilidad` | `INCIDENTE_VIAL` en `EspacioPublicoAfectado` |
| `AlertaClimaticaGenerada` | propio | `EVENTO_CLIMATICO` en `EventoPublicoActualizado` (cancelación) |

## 4. Caso 5 (CU-01 — emergencia durante un viaje en bici)

Sin cambios: `espacios.reserva.afectada` (flujo A10), reconocido por `correlationId =
emergenciaId`. `espacios.espacio.liberado` no se cuenta (mismo incidente).

## 5. Métricas que salen con esto

| Caso | Métrica | Cómo la calculamos |
|---|---|---|
| 1 | Eventos culturales cancelados por un evento climático | `causaTipo = EVENTO_CLIMATICO`|
| 2 | Espacios públicos afectados a causa de un reclamo | `EspacioPublicoAfectado`: `causaTipo = RECLAMO`; `espacioId` distintos |
| 3 | Cantidad de eventos masivos creados | `ActividadCreada` con `capacidad >= 500` |
| 4 | Espacios públicos afectados por incidentes | `EspacioPublicoAfectado`: `causaTipo = INCIDENTE_VIAL`; `espacioId` distintos |
| 5 | actividad modificadas por accidentes viales | causaTipo = ACCIDENTE_VIAL en ActividadModificada|
| 6 | Actividades de ciudadanos vs. eventos totales | organizadorTipo en ActividadCreada CIUADANO o CIUDAD |

## 6. Pendientes

- `tipoEvento` de la alerta climática depende de la fuente que se integre.
- Umbral de `masivo` (propuesto: `cupoMaximo ≥ 1000`).
- Definición de "eventos totales" del caso 6.
