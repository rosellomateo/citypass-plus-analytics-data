## 2. Qué tienen que publicar

### 2.1 `EstacionUrbanaAfectada` — ya existe (verificado, schemaId 69)

| Campo | Formato | Notas |
|---|---|---|
| `afectacionId` | string | |
| `estacionId` | string | |
| `latitud` / `longitud` | double | |
| `tipoAfectacion` | enum | `BLOQUEADA`, `CAPACIDAD_REDUCIDA`, `FUERA_DE_SERVICIO`, `RECOMENDACION_RESTRINGIDA` |
| `afectacionDesde` | string (ISO-8601) | puede ser **futuro**: afectación anunciada preventivamente (ej. por un evento masivo) |
| `afectacionHastaEstimada` | string \| null | |
| `causaId` | string | obligatorio |
| `causaTipo` | enum | obligatorio. Valores: los 6 de la tabla 1.1 |
| `moduloOrigen` | enum | `RECLAMOS`, `EMERGENCIAS`, `MOVILIDAD` — qué módulo originó la afectación (no estaba en la versión anterior del contrato) |
| `correlationId` | string | |
| `occurredAt` | string (ISO-8601) | cuándo se publicó |

```json
{
  "afectacionId": "e71b4c2a-9d3f-4a1e-8c5b-2f6d0a9e3b77",
  "estacionId": "EST-042",
  "latitud": -34.5711,
  "longitud": -58.4233,
  "tipoAfectacion": "BLOQUEADA",
  "afectacionDesde": "2026-11-15T16:00:00.000Z",
  "afectacionHastaEstimada": "2026-11-16T02:00:00.000Z",
  "causaId": "f2d8a6b1-4c7e-4b9a-a3d5-8e1f0c6b2a99",
  "causaTipo": "EVENTO_PUBLICO",
  "moduloOrigen": "MOVILIDAD",
  "correlationId": "c-5520",
  "occurredAt": "2026-11-12T10:00:00.000Z"
}
```

### 2.2 `EstacionUrbanaRestaurada` — ya existe (verificado, schemaId 70)

| Campo | Formato | Notas |
|---|---|---|
| `afectacionId` | string | el de la `EstacionUrbanaAfectada` que cierra |
| `estacionId` | string | |
| `correlationId` | string | |
| `restauradaAt` | string (ISO-8601) | |

### 2.3 `ViajeFinalizado` — NUEVO, no existe todavía (a confirmar con ustedes)

No hay en el bus ningún evento de viaje hoy (ni `ViajeIniciado`, ni `ViajeTerminado`). Para no
depender de un join entre dos eventos que no existen, proponemos **un solo evento** con el viaje
completo (inicio + fin), publicado cuando termina:

| Campo | Formato | Notas |
|---|---|---|
| `viajeId` | string | |
| `correlationId` | string | |
| `estacionInicio` | string | |
| `latitudInicio` / `longitudInicio` | double | |
| `horaInicio` | string (ISO-8601) | |
| `estacionFin` | string | |
| `latitudFin` / `longitudFin` | double | |
| `horaFinalizacion` | string (ISO-8601) | |
| `causaId` / `causaTipo` | string \| null / enum \| null | opcional — se completa si el viaje terminó por un incidente (`INCIDENTE_VIAL`), para el caso 5 |
| `createdAt` | string (ISO-8601) | |

**Propuesta, a confirmar con ustedes:**
- ¿Les sirve este esquema de un solo evento, o prefieren seguir publicando `ViajeIniciado` +
  `ViajeTerminado` por separado? Si es así, avísennos y lo adaptamos.
- ¿Tienen manera de saber que un viaje terminó por un incidente vial (para completar
  `causaTipo = INCIDENTE_VIAL`), o eso lo determina otro módulo?



## 4. Caso 5 (CU-01 — viaje interrumpido por incidente vial)

Depende de que `ViajeFinalizado` (sección 2.3) exista y complete `causaTipo = INCIDENTE_VIAL`
cuando corresponda. Mientras no exista, este caso queda bloqueado.

## 5. Métricas que salen con esto

| Caso | Métrica | Cómo la calculamos |
|---|---|---|
| 1 | Estaciones bloqueadas por un evento climático | `EstacionUrbanaAfectada`: `tipoAfectacion = BLOQUEADA`, `causaTipo = EVENTO_CLIMATICO`; `estacionId` distintos |
| 2 | Estaciones afectadas por reclamos | `causaTipo = RECLAMO` |
| 3 | Estaciones que se sabe que serán afectadas por el evento | `causaTipo = EVENTO_PUBLICO` y `afectacionDesde > occurredAt` |
| 4 | Estaciones afectadas por incidentes | `causaTipo = INCIDENTE_VIAL` |
| 5 | Viajes interrumpidos por accidente vs. finalizados normalmente | `ViajeFinalizado` `causaTipo = INCIDENTE_VIAL` (bloqueado hasta que exista el evento) |
| 6 | Estaciones afectadas por eventos de ciudadanos | `causaTipo = RESERVA` |

