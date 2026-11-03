# Modelado de eventos — Parte 2 (comunicación entre módulos)

> Fuente: `Modelado eventos.pdf` (6 casos de uso cruzados, 30 métricas — una por caso de uso ×
> los 5 módulos existentes). Objetivo: modelar los eventos necesarios para poder calcular esas
> 30 métricas, cambiando lo menos posible el modelo de Parte 1.
>
> **Revisión 29/oct**: se ajustaron los eventos a las métricas modificadas. Qué cambió y por
> qué está en §8. Los contratos que se le mandan a cada equipo salen de este documento
> (`contratos-parte2/CONTRATO_*.md`).

## Métricas por caso de uso (referencia)

Las 30 métricas tal como las definió el grupo, agrupadas por los 6 casos de uso transversales.
Esta sección es solo de referencia (no cambia según las decisiones de diseño) — el resto del
documento explica cómo se calcula cada una.

### Caso 1 — Evento climático que afecta varios servicios

| Módulo | Métrica |
|---|---|
| Espacios | Eventos culturales cancelados por evento climático |
| Residuos | Cuadrillas afectadas por eventos climáticos |
| Reclamos | Reclamos realizados a causa de un evento climático |
| Emergencias | Emergencias creadas con su criticidad afectada por algún evento climático |
| Movilidad | Estaciones bloqueadas a causa de un evento climático |

### Caso 2 — Reclamo ciudadano que desencadena una intervención urbana

*(ej: un ciudadano reporta un estacionamiento indebido que bloquea una ciclovía)*

| Módulo | Métrica |
|---|---|
| Espacios | Espacios públicos afectados a causa de un reclamo |
| Residuos | Escuadrillas enviadas a causa de un reclamo ciudadano, por barrio |
| Reclamos | Cantidad de reclamos ciudadanos que afecten a los demás módulos |
| Emergencias | Emergencias creadas a causa de un reclamo ciudadano |
| Movilidad | Estaciones afectadas por reclamos ciudadanos |

### Caso 3 — Recolección inteligente después de un evento masivo

| Módulo | Métrica |
|---|---|
| Espacios | Cantidad de eventos masivos creados |
| Residuos | Cuadrillas planificadas para las inmediaciones del evento |
| Reclamos | Cantidad de reclamos ciudadanos relacionados al evento en cuestión |
| Emergencias | Servicios públicos enviados (policía, bomberos, etc.) al evento, de manera preventiva |
| Movilidad | Estaciones de bicicleta que se sabe que serán afectadas por el evento |

### Caso 4 — Ruta urbana inteligente con incidentes en tiempo real

| Módulo | Métrica |
|---|---|
| Espacios | Cantidad de espacios públicos afectados por incidentes |
| Residuos | Cantidad de escuadrillas afectadas por incidentes |
| Reclamos | Cantidad de reclamos creados asociados a incidentes que están programados |
| Emergencias | Emergencias creadas por incidentes programados, recursos enviados por incidente programado |
| Movilidad | Cantidad de estaciones afectadas por incidentes |

### Caso 5 — Emergencia durante un viaje en bicicleta

| Módulo | Métrica |
|---|---|
| Espacios | Reservas afectadas por un accidente en bici ocurrido cerca |
| Residuos | Intervenciones programadas a raíz de este tipo de accidente, comparado con intervenciones por alertas normales |
| Reclamos | Reclamos de la vía originados por este tipo de accidente, comparado con reclamos de vía por otras causas |
| Emergencias | Emergencias originadas durante un viaje en bicicleta, con tiempo promedio hasta resolución |
| Movilidad | Viajes interrumpidos por accidente en bicicleta, comparado con viajes finalizados normalmente |

### Caso 6 — Reserva de espacio público + movilidad + residuos

*(ej: un ciudadano reserva una plaza para realizar un evento comunitario)*

| Módulo | Métrica |
|---|---|
| Espacios | Reservas de lugares públicos hechas por ciudadanos vs. eventos totales |
| Residuos | Cantidad de escuadrillas enviadas cerca de la zona del espacio público |
| Reclamos | Reclamos producidos relacionados al evento |
| Emergencias | Emergencias creadas por eventos realizados por ciudadanos |
| Movilidad | Cantidad de estaciones afectadas por eventos ciudadanos |

> "Cuadrilla" y "escuadrilla" se usan como sinónimos. En los eventos, una cuadrilla se
> identifica por `camionId` (camión + dotación), igual que en `RecoleccionCompletada` de Parte 1.

## 1. Principio de diseño: un patrón único de causalidad

Mirando las 30 métricas, la gran mayoría tiene la misma forma: **"cantidad de X creados/afectados
a causa de Y"**, donde X es la entidad propia del módulo (reclamo, emergencia, intervención,
estación, espacio, evento) e Y es uno de un puñado de orígenes posibles.

En vez de inventar un campo de "causa" distinto por cada combinación módulo→módulo, se usa **un
solo par de campos reusable**:

| Campo | Tipo | Notas |
|---|---|---|
| `causaId` | string \| null | El id de la entidad que originó este evento. `null` = orgánico (lo generó un ciudadano/el flujo normal, sin causa externa). |
| `causaTipo` | enum \| null | Dice a qué entidad pertenece `causaId` (y por lo tanto, en qué tabla buscarlo). `null` si y solo si `causaId` es `null`. |

### 1.1 Valores de `causaTipo`

| `causaTipo` | `causaId` es… | Evento origen | Lo publica |
|---|---|---|---|
| `EVENTO_CLIMATICO` | `alertaClimaticaId` | `AlertaClimaticaGenerada` | Espacios |
| `RECLAMO` | `reclamoId` | `ReclamoCreado` | Reclamos |
| `INCIDENTE_VIAL` | `incidenteId` | `IncidenteVialCreado` | Movilidad |
| `EVENTO_PUBLICO` | `eventoId` | `EventoPublicoCreado` | Espacios |
| `RESERVA` | `reservaId` (de una reserva `tipoReserva = ESPACIO`) | `ReservaCreada` | Espacios |

`EVENTO_PUBLICO` (caso 3, evento masivo de la agenda) y `RESERVA` (caso 6, un ciudadano reserva
un espacio) son valores distintos a propósito: así cada caso sale con un único filtro y no hay
ambigüedad sobre qué id va en `causaId` (un evento tiene muchas inscripciones/`reservaId`, pero un
solo `eventoId`).

### 1.2 Dónde va la causa

- En un evento de **creación** (`ReclamoCreado`, `EmergenciaCreada`, `IntervencionProgramada`,
  …), la causa dice **por qué se creó** la entidad.
- En un evento de **afectación/actualización** (`IntervencionActualizada`,
  `EventoPublicoActualizado`, `EstacionUrbanaAfectada`, `EspacioPublicoAfectado`), la causa dice
  **por qué se afectó**. Es independiente de la causa de creación: una intervención programada
  normalmente (`causaTipo = null`) puede después quedar `AFECTADA` por una tormenta.

### 1.3 Cómo decide cada módulo la causa

Cada módulo decide cuándo completar la causa con su propia regla de negocio (un operador que
vincula a mano, un reclamo derivado, cercanía en espacio y tiempo con una alerta o un incidente
activo, etc.). Para eso tiene que **consumir los eventos origen** de la tabla 1.1 que le
correspondan (detalle en cada contrato). Analítica no infiere causas: solo cuenta lo que viene en
el evento.

**Importante**: esto es un campo *nuevo*, distinto de `eventoOrigenId` que ya tiene `ReclamoCreado`
hoy — ese es para idempotencia (el `eventId` del propio evento, para detectar reentregas), no para
causalidad entre módulos. No se tocan ni se pisan.

## 2. Geolocalización en vez de zona/barrio

Para todos los eventos **nuevos** que se listan en este documento, se usa `latitud`/`longitud`
en vez de `zona`/`barrio`. Motivo: las métricas de "afectados cerca de" y los cruces entre módulos
por proximidad necesitan coordenadas, no un nombre de barrio.

Los eventos de Parte 1 (`ReclamoCreado.barrio`, `AlertaGenerada.zona`, `EmergenciaCreada.barrio`,
`ViajeIniciado.barrioInicio`, etc.) **no se tocan**. La única excepción es `ReservaCreada`
(nuestra), que reemplaza `zona` por `latitud`/`longitud` (§4.4).

Cuando una métrica pide agrupar "por barrio" sobre un evento nuevo (caso 2 de Residuos), el
barrio se toma de la entidad causa (`ReclamoCreado.barrio` vía `causaId`), no se agrega un campo.

## 3. Eventos origen nuevos

Son los eventos que los demás módulos referencian en `causaId`. Todos llevan `correlationId` y
timestamps ISO-8601.

### 3.1 `AlertaClimaticaGenerada` — Espacios (caso 1)

| Campo | Formato | Notas |
|---|---|---|
| `alertaClimaticaId` | uuid | |
| `tipoEvento` | enum | depende de la fuente climática que se integre (ver §6). Propuesta: `TORMENTA`, `LLUVIA_INTENSA`, `VIENTO_FUERTE`, `CALOR_EXTREMO`, `GRANIZO`, `INUNDACION` |
| `severidad` | enum | `ALTA`, `MEDIA`, `BAJA` |
| `latitud` / `longitud` | double | centro del área afectada |
| `radioAfectacionKm` | double \| null | `null` = toda la ciudad |
| `descripcion` | string | |
| `vigenteDesde` | datetime ISO-8601 | |
| `vigenteHastaEstimado` | datetime ISO-8601 \| null | ventana que usan los otros módulos para decidir si algo "es a causa" de la alerta |
| `correlationId` | string | |
| `occurredAt` | datetime ISO-8601 | |

No necesita evento de "actualizada" para las métricas. Si más adelante se necesita saber cuándo
"termina" la alerta, se agrega `AlertaClimaticaFinalizada`.

### 3.2 `IncidenteVialCreado` / `IncidenteVialActualizado` — Movilidad (caso 4)

El caso 4 necesita un origen de "incidente" que hoy no existe. Lo origina **Movilidad Urbana**
(es sobre la red vial/rutas). Mismo patrón Creado/Actualizado que Reclamos y Emergencias.

**`IncidenteVialCreado`**

| Campo | Formato | Notas |
|---|---|---|
| `incidenteId` | uuid | |
| `tipoIncidente` | enum | `OBRA`, `CORTE_CALLE`, `SEMAFORO_DANADO`, `ACCIDENTE_TRANSITO`, `ARBOL_CAIDO` |
| `estado` | enum | `PROGRAMADO` (se conoce de antemano: obra, corte) o `ACTIVO` (ocurre en tiempo real) |
| `latitud` / `longitud` | double | |
| `inicioPrevisto` | datetime ISO-8601 \| null | obligatorio si `estado = PROGRAMADO` |
| `duracionEstimadaMinutos` | int \| null | |
| `causaId` / `causaTipo` | — | opcional (ej. árbol caído por tormenta → `EVENTO_CLIMATICO`); ninguna métrica lo usa |
| `correlationId` | string | |
| `createdAt` | datetime ISO-8601 | |

**`IncidenteVialActualizado`**

| Campo | Formato | Notas |
|---|---|---|
| `incidenteId` | uuid | |
| `estadoAnterior` / `estadoNuevo` | enum | `PROGRAMADO` → `ACTIVO` → `RESUELTO` (+ `CANCELADO` desde `PROGRAMADO`) |
| `correlationId` | string | |
| `updatedAt` | datetime ISO-8601 | |

"Incidente programado" (casos 4 de Reclamos y Emergencias) = incidente cuyo `IncidenteVialCreado`
vino con `estado = PROGRAMADO`.

### 3.3 `EventoPublicoCreado` / `EventoPublicoActualizado` — Espacios (casos 1 y 3)

Hoy la creación de un evento de la agenda no se publica: solo existen las inscripciones
(`ReservaCreada` con `tipoReserva = EVENTO`). Sin este evento los demás módulos no tienen forma de
enterarse de un evento masivo para planificar (Residuos, Emergencias, Movilidad) ni un id único
para referenciarlo.

**`EventoPublicoCreado`**

| Campo | Formato | Notas |
|---|---|---|
| `eventoId` | uuid | el mismo id que va en `ReservaCreada.recursoId` de las inscripciones |
| `espacioId` | string | dónde se hace |
| `nombre` | string | |
| `categoria` | enum | `CULTURAL`, `DEPORTIVO`, `RECREATIVO` |
| `latitud` / `longitud` | double | |
| `fechaInicio` / `fechaFin` | datetime ISO-8601 | |
| `cupoMaximo` | int \| null | |
| `masivo` | boolean | lo decide Espacios (umbral propuesto: `cupoMaximo ≥ 1000`), así todos los módulos usan el mismo criterio |
| `organizadorId` | string | |
| `correlationId` | string | |
| `createdAt` | datetime ISO-8601 | |

**`EventoPublicoActualizado`**

| Campo | Formato | Notas |
|---|---|---|
| `eventoId` | uuid | |
| `estadoAnterior` / `estadoNuevo` | enum | `PROGRAMADO` → `EN_CURSO` → `FINALIZADO` (+ `CANCELADO` desde `PROGRAMADO`/`EN_CURSO`) |
| `motivo` | string \| null | |
| `causaId` / `causaTipo` | — | por qué se canceló (ej. `EVENTO_CLIMATICO`) |
| `correlationId` | string | |
| `updatedAt` | datetime ISO-8601 | |

## 4. Cambios por módulo existente

### 4.1 Reclamos

**Sin cambios**: toda la máquina de estados y los campos actuales.

**Campo nuevo** en `ReclamoCreado`: `causaId`, `causaTipo` (ambos `null` en el flujo normal).

| Caso | Métrica | Cómo se resuelve |
|---|---|---|
| 1 | Reclamos a causa de evento climático | `causaTipo = EVENTO_CLIMATICO` |
| 2 | Reclamos que afectan a otros módulos | **No sale de Reclamos** — es la cuenta inversa: `reclamoId` distintos que aparecen como `causaId` (con `causaTipo = RECLAMO`) en eventos de otros módulos. Ver §5 (gold cruzado). |
| 3 | Reclamos relacionados al evento [masivo] | `causaTipo = EVENTO_PUBLICO` |
| 4 | Reclamos asociados a incidentes programados | `causaTipo = INCIDENTE_VIAL` y el incidente se creó con `estado = PROGRAMADO` |
| 5 | Reclamos de vía por accidente de bici vs. otras causas | Ver §7. `reclamos.creado` del CU-01 (A5), reconocido por `correlationId = emergenciaId`, vs. reclamos de vía normales (`categoria IN (BACHES, TRANSITO)`, a confirmar). |
| 6 | Reclamos relacionados al evento comunitario | `causaTipo = RESERVA` |

### 4.2 Movilidad Urbana

**Eventos nuevos**: `IncidenteVialCreado` / `IncidenteVialActualizado` (§3.2) y
`EstacionUrbanaAfectada` / `EstacionUrbanaRestaurada` (no hay forma de modelar "estación afectada"
con lo que existe hoy).

**`EstacionUrbanaAfectada`**

| Campo | Formato | Notas |
|---|---|---|
| `afectacionId` | uuid | |
| `estacionId` | string | mismo id que `ViajeIniciado.estacion` |
| `latitud` / `longitud` | double | |
| `tipoAfectacion` | enum | `BLOQUEADA`, `CAPACIDAD_REDUCIDA`, `FUERA_DE_SERVICIO` |
| `afectacionDesde` | datetime ISO-8601 | puede ser futuro: afectación anunciada preventivamente (caso 3) |
| `afectacionHastaEstimada` | datetime ISO-8601 \| null | |
| `causaId` / `causaTipo` | — | casi siempre completo, es la razón de ser del evento |
| `correlationId` | string | |
| `occurredAt` | datetime ISO-8601 | cuándo se publicó |

**`EstacionUrbanaRestaurada`**

| Campo | Formato | Notas |
|---|---|---|
| `afectacionId` | uuid | el de la `EstacionUrbanaAfectada` que cierra |
| `estacionId` | string | |
| `correlationId` | string | |
| `restauradaAt` | datetime ISO-8601 | |

**Caso 5**: no se agregan campos ni eventos. Se usa `movilidad.viaje.actualizado` del CU-01. Detalle
en §7.

| Caso | Métrica | Cómo se resuelve |
|---|---|---|
| 1 | Estaciones bloqueadas por evento climático | `EstacionUrbanaAfectada`: `tipoAfectacion = BLOQUEADA`, `causaTipo = EVENTO_CLIMATICO`; `estacionId` distintos |
| 2 | Estaciones afectadas por reclamos | `causaTipo = RECLAMO` |
| 3 | Estaciones que se sabe serán afectadas por el evento | `causaTipo = EVENTO_PUBLICO` con `afectacionDesde > occurredAt` (anunciada antes de que ocurra) |
| 4 | Estaciones afectadas por incidentes | `causaTipo = INCIDENTE_VIAL` |
| 5 | Viajes interrumpidos por accidente vs. finalizados normalmente | Ver §7: `movilidad.viaje.actualizado` del CU-01 vs. `ViajeTerminado` |
| 6 | Estaciones afectadas por eventos ciudadanos | `causaTipo = RESERVA` |

### 4.3 Gestión de Residuos Inteligente

**Sin cambios**: `AlertaGenerada` y `RecoleccionCompletada` quedan exactamente igual.

Los casos de residuos son todos sobre *planificación* ("cuadrillas enviadas/planificadas/
afectadas"), algo que el modelo de Parte 1 no cubre. En vez de un evento nuevo propio
(`RecoleccionProgramada`, propuesto en la versión anterior), se **extiende el evento que Residuos
ya definió para el CU-01**, `IntervencionProgramada`, y se agrega su actualización. Así Residuos
publica un solo evento de planificación para todos los casos.

**`IntervencionProgramada`** (existente — se agregan campos)

| Campo | Formato | Notas |
|---|---|---|
| `intervencionId` | uuid | existente |
| `contenedorId` | string | existente |
| `latitud` / `longitud` | double | existente |
| `emergenciaId` | uuid \| null | existente (CU-01) |
| `programadaEn` | datetime ISO-8601 | existente |
| `correlationId` | string | existente |
| `camionId` | string | **nuevo** — identifica la cuadrilla |
| `alertaId` | uuid \| null | **nuevo** — si se programó por una `AlertaGenerada` (base de comparación del caso 5) |
| `causaId` / `causaTipo` | — | **nuevo** — por qué se programó. `null` = recorrido normal o alerta propia |

**`IntervencionActualizada`** (nuevo)

| Campo | Formato | Notas |
|---|---|---|
| `intervencionId` | uuid | |
| `camionId` | string | |
| `estadoAnterior` / `estadoNuevo` | enum | `PROGRAMADA` → `COMPLETADA` \| `AFECTADA` \| `CANCELADA` |
| `causaId` / `causaTipo` | — | por qué quedó `AFECTADA`/`CANCELADA` (independiente de la causa de la programación) |
| `correlationId` | string | |
| `updatedAt` | datetime ISO-8601 | |

| Caso | Métrica | Cómo se resuelve |
|---|---|---|
| 1 | Cuadrillas afectadas por clima | `IntervencionActualizada`: `estadoNuevo = AFECTADA`, `causaTipo = EVENTO_CLIMATICO`; `camionId` distintos |
| 2 | Cuadrillas enviadas a causa de reclamo (por barrio) | `IntervencionProgramada`: `causaTipo = RECLAMO`; barrio = `ReclamoCreado.barrio` del `causaId` |
| 3 | Cuadrillas planificadas para el evento masivo | `IntervencionProgramada`: `causaTipo = EVENTO_PUBLICO`; `camionId` distintos |
| 4 | Cuadrillas afectadas por incidentes | `IntervencionActualizada`: `estadoNuevo = AFECTADA`, `causaTipo = INCIDENTE_VIAL` |
| 5 | Programadas por accidente vs. por alertas normales | Ver §7: `IntervencionProgramada` con `emergenciaId` no nulo vs. con `alertaId` no nulo |
| 6 | Cuadrillas enviadas cerca del espacio público | `IntervencionProgramada`: `causaTipo = RESERVA` (la cercanía la decide Residuos al vincular) |

### 4.4 Espacios Públicos y Cultura

Este es nuestro módulo, así que acá entran los cambios más grandes: publicamos dos de los cinco
orígenes (`AlertaClimaticaGenerada`, `EventoPublicoCreado`, §3) y somos el único módulo de los 5
que hoy no tiene geolocalización.

**Campo reemplazado** en `ReservaCreada`: `zona` → `latitud` / `longitud`. Necesario para el caso
6: Residuos y Movilidad tienen que ubicar el espacio reservado para decidir qué contenedores y
estaciones quedan afectados.

**`ReservaActualizada`: sin cambios.** La versión anterior le agregaba `causaId`/`causaTipo`, pero
con `EventoPublicoActualizado` y `EspacioPublicoAfectado` ninguna métrica lo necesita.

**Evento nuevo** `EspacioPublicoAfectado` / `EspacioPublicoRestaurado` (casos 2 y 4: las métricas
cuentan *espacios*, y un espacio puede quedar afectado sin tener ninguna reserva; mismo patrón que
`EstacionUrbanaAfectada`):

**`EspacioPublicoAfectado`**

| Campo | Formato | Notas |
|---|---|---|
| `afectacionId` | uuid | |
| `espacioId` | string | |
| `categoria` | enum | `PLAZA`, `SALON`, `CANCHA`, `AUDITORIO` |
| `latitud` / `longitud` | double | |
| `tipoAfectacion` | enum | `CERRADO`, `USO_PARCIAL`, `ACCESO_RESTRINGIDO` |
| `afectacionDesde` / `afectacionHastaEstimada` | datetime ISO-8601 (hasta: \| null) | |
| `causaId` / `causaTipo` | — | |
| `correlationId` | string | |
| `occurredAt` | datetime ISO-8601 | |

**`EspacioPublicoRestaurado`**: `afectacionId`, `espacioId`, `correlationId`, `restauradoAt`.

| Caso | Métrica | Cómo se resuelve |
|---|---|---|
| 1 | Eventos culturales cancelados por clima | `EventoPublicoActualizado`: `estadoNuevo = CANCELADO`, `causaTipo = EVENTO_CLIMATICO`, y `categoria = CULTURAL` (de `EventoPublicoCreado`) |
| 2 | Espacios afectados por un reclamo | `EspacioPublicoAfectado`: `causaTipo = RECLAMO`; `espacioId` distintos |
| 3 | Cantidad de eventos masivos creados | `EventoPublicoCreado` con `masivo = true` |
| 4 | Espacios públicos afectados por incidentes | `EspacioPublicoAfectado`: `causaTipo = INCIDENTE_VIAL`; `espacioId` distintos |
| 5 | Reservas afectadas por accidente de bici cerca | Ver §7: cantidad de `espacios.reserva.afectada` del CU-01 (A10). `espacios.espacio.liberado` no se cuenta, para no contar dos veces el mismo incidente. |
| 6 | Reservas de ciudadanos vs. eventos totales | Numerador: `ReservaCreada` con `tipoReserva = ESPACIO` que llegaron a `CONFIRMADA`. Denominador: numerador + `EventoPublicoCreado` del período (a validar, ver §6). |

### 4.5 Emergencias

**Sin cambios de estados**: el ciclo PENDIENTE→...→CERRADA/DESCARTADA queda igual.

**Campos nuevos** en `EmergenciaCreada`:

| Campo | Formato | Notas |
|---|---|---|
| `causaId` / `causaTipo` | — | |
| `prioridadOriginal` | enum \| null | `ALTA`, `MEDIA`, `BAJA`. Prioridad que se le habría asignado sin el evento climático. Obligatorio si `causaTipo = EVENTO_CLIMATICO` (igual a `prioridad` si no hubo ajuste); `null` en el resto. Sale de la métrica del caso 1 ("criticidad afectada"). |

**Valor nuevo** en `EmergenciaCreada.categoria`: `PREVENTIVA` — despliegue preventivo (caso 3), para
distinguirlo de una emergencia real ocurrida durante el evento.

| Caso | Métrica | Cómo se resuelve |
|---|---|---|
| 1 | Emergencias creadas con su criticidad afectada por clima | `causaTipo = EVENTO_CLIMATICO`; "criticidad afectada" = `prioridad ≠ prioridadOriginal`. Se muestra el total y cuántas subieron de prioridad. |
| 2 | Emergencias a causa de un reclamo | `causaTipo = RECLAMO` |
| 3 | Servicios enviados preventivamente al evento | `EmergenciaCreada`: `causaTipo = EVENTO_PUBLICO`, `categoria = PREVENTIVA`; servicio = `areaResponsable` de la `EmergenciaActualizada` con `estadoNuevo = DESPACHADA` |
| 4 | Emergencias / recursos por incidentes programados | `causaTipo = INCIDENTE_VIAL` y el incidente se creó con `estado = PROGRAMADO`; recursos = `EmergenciaActualizada` con `estadoNuevo = DESPACHADA` (`asignadoA`, `areaResponsable`) |
| 5 | Emergencias durante un viaje en bici, tiempo prom. hasta resolución | Ver §7. Sin `causaTipo`: el vínculo con el viaje viene en `emergencia.solicitada` (`viajeActivoInformado`) y en `emergencia.resuelta` (`tripId`). |
| 6 | Emergencias por eventos ciudadanos | `causaTipo = RESERVA` |

### 4.6 Matriz de causas usadas por las métricas

Cualquier evento con `causaId`/`causaTipo` acepta cualquier valor del enum; esta matriz es el
mínimo que cada módulo tiene que completar para que salgan las métricas.

| Evento | Publica | `EVENTO_CLIMATICO` | `RECLAMO` | `INCIDENTE_VIAL` | `EVENTO_PUBLICO` | `RESERVA` |
|---|---|---|---|---|---|---|
| `ReclamoCreado` | Reclamos | ✓ (c1) | | ✓ (c4) | ✓ (c3) | ✓ (c6) |
| `EmergenciaCreada` | Emergencias | ✓ (c1) | ✓ (c2) | ✓ (c4) | ✓ (c3) | ✓ (c6) |
| `IntervencionProgramada` | Residuos | | ✓ (c2) | | ✓ (c3) | ✓ (c6) |
| `IntervencionActualizada` | Residuos | ✓ (c1) | | ✓ (c4) | | |
| `EstacionUrbanaAfectada` | Movilidad | ✓ (c1) | ✓ (c2) | ✓ (c4) | ✓ (c3) | ✓ (c6) |
| `EspacioPublicoAfectado` | Espacios | | ✓ (c2) | ✓ (c4) | | |
| `EventoPublicoActualizado` | Espacios | ✓ (c1) | | | | |

## 5. Impacto en nuestra arquitectura (bronze/silver/gold)

1. **Bronze**: sin cambios de infraestructura — los eventos nuevos caen solos en la carpeta de su
   dominio existente (`DOMINIOS` en `bp_data_ingestion_bronze.py`). `AlertaClimaticaGenerada`,
   `EventoPublicoCreado/Actualizado` y `EspacioPublicoAfectado/Restaurado` caen bajo "Espacios
   Publicos y Cultura"; `IncidenteVialCreado/Actualizado` y `EstacionUrbanaAfectada/Restaurada`
   bajo "Movilidad Urbana"; `IntervencionProgramada/Actualizada` bajo "Gestion de Residuos
   Inteligente".
2. **Silver**: cada dominio que recibe `causaId`/`causaTipo` los suma como columnas a su tabla (no
   cambia el patrón de upsert). Los eventos realmente nuevos van a su propia tabla de silver dentro
   del dominio (ej. Espacios: `reservas.parquet`, `alertas_climaticas.parquet`,
   `eventos_publicos.parquet`, `espacios_afectados.parquet`) — mismo patrón de blueprint, solo que
   un dominio puede terminar con más de un par silver/gold.
3. **Gold**: la mayoría de las métricas salen agrupando por `causaTipo` dentro del gold de cada
   dominio. Hay tres que necesitan **cruzar dominios**, lo que justifica un
   `bp_analitica_cruzada_gold.py` que lea las tablas de silver de todos los módulos:
   - Reclamos c2: `reclamoId` distintos referenciados como `causaId` en los demás módulos.
   - Reclamos c4 / Emergencias c4: filtrar por incidentes con `estado = PROGRAMADO` (tabla de
     Movilidad).
   - Residuos c2: barrio del reclamo causa (tabla de Reclamos).

## 6. Abierto / a validar con el resto de los grupos

- **Nombres y valores del enum `causaTipo`** (§1.1): es la pieza de la que depende todo lo demás;
  cerrarla primero con todos los grupos.
- `tipoEvento` de `AlertaClimaticaGenerada`: depende de qué fuente climática se integre.
- Umbral de `masivo` en `EventoPublicoCreado` (propuesto `cupoMaximo ≥ 1000`).
- Caso 6 de Espacios: qué cuenta como "eventos totales" (propuesto: reservas ciudadanas de
  espacios + eventos públicos creados).
- Caso 5 de Reclamos: qué categorías son "reclamos de vía" (propuesto `BACHES`, `TRANSITO`).
- Caso 5 de Residuos: confirmar con Residuos que la base de comparación son las intervenciones con
  `alertaId` no nulo.
- Emergencias: confirmar el valor `PREVENTIVA` en `categoria` y el campo `prioridadOriginal`.
- Residuos: confirmar que aceptan extender `IntervencionProgramada` (en vez de un evento nuevo).

## 7. Caso 5 — cómo modelarlo según el documento del CU-01

Para el caso 5 nos adaptamos a lo que ya define el documento del caso de uso
(`Caso de uso detallado.docx`). **No se le piden campos ni eventos nuevos a ningún módulo** para
este caso, y el patrón `causaId`/`causaTipo` de §1 no se usa. Los nombres de eventos y campos de
esta sección son exactamente los del documento del CU-01; si allá cambian, se cambian acá.

**Rol de Analítica:** no publica eventos ni participa del flujo en vivo. Lee los eventos del caso
y les manda a los módulos afectados gráficos HTML con lo que cambió en su dominio (paso 16 del
CU-01).

**Cómo se reconocen los eventos del caso:** el CU-01 dice que todos sus eventos viajan con
`correlationId = emergenciaId`. Con eso alcanza para saber que un reclamo, una reserva afectada o
una intervención de Residuos vienen de esta emergencia. La hora de cada evento sale del campo
`time` del sobre.

### 7.1 Eventos a consumir y campos que usamos

| Tópico (doc CU-01) | Publica | Campos que usamos | Para qué |
|---|---|---|---|
| `emergencia.solicitada` | Emergencias | `emergenciaId`, `viajeActivoInformado` (null si no había viaje activo, A8) | saber si la emergencia ocurrió durante un viaje |
| `emergencia.creada` | Emergencias | `correlationId`, `time` | inicio del tiempo hasta resolución |
| `emergencia.resuelta` | Emergencias | `emergenciaId`, `tripId`, `resultado`, `time` | fin del tiempo hasta resolución |
| `emergencia.cancelada` | Emergencias | `correlationId` | excluir falsas alarmas (A1) |
| `movilidad.viaje.actualizado` | Movilidad | `correlationId` | viaje finalizado sin cargo por la emergencia |
| `reclamos.creado` (A5) | Reclamos | `correlationId`, `categoria` | reclamos de la vía causados por el accidente |
| `espacios.reserva.afectada` (A10) | Espacios | `correlationId` | reservas afectadas por el accidente |
| `residuos.intervencion.programada` (A7) | Residuos | `intervencionId`, `emergenciaId`, `alertaId`, `contenedorId`, `latitud`, `longitud`, `programadaEn`, `correlationId` | intervenciones por el accidente y base de comparación |

No se usan para las métricas: `movilidad.viaje.iniciado`, `movilidad.viaje.interrumpido` (con una
falsa alarma el viaje se reanuda, así que contaría de más), `emergencia.estado.actualizado`,
`eda.anomalia.detectada`, `espacios.espacio.liberado` (es el mismo incidente que la reserva
afectada), `espacios.problematica.registrada`.

### 7.2 Cómo sale cada métrica

| Módulo | Métrica | Cálculo |
|---|---|---|
| Movilidad | Viajes interrumpidos por accidente en bici vs. finalizados normalmente | cantidad de `movilidad.viaje.actualizado` del CU-01 vs. `ViajeTerminado` (eventos normales de Movilidad) |
| Emergencias | Emergencias originadas durante un viaje en bici, con tiempo promedio hasta resolución | emergencias con `viajeActivoInformado` distinto de null; tiempo = `time` de `emergencia.creada` → `time` de `emergencia.resuelta`, sin las que tienen `emergencia.cancelada` |
| Reclamos | Reclamos de la vía por este accidente vs. reclamos de vía por otras causas | `reclamos.creado` cuyo `correlationId` es un `emergenciaId` del CU-01 vs. resto de `ReclamoCreado` con `categoria IN (BACHES, TRANSITO)` |
| Espacios | Reservas afectadas por un accidente en bici ocurrido cerca | cantidad de `espacios.reserva.afectada` |
| Residuos | Intervenciones por este accidente vs. por alertas normales | `IntervencionProgramada` con `emergenciaId` no nulo vs. con `alertaId` no nulo |

### 7.3 Nombres de eventos

Los tópicos de arriba son los del documento del CU-01. En el gateway, Residuos publica su evento
como `com.citypass.residuos.IntervencionProgramada` y escucha la resolución como
`EmergenciaActualizada` con estado `RESUELTA` (aceptan también `emergencia.resuelta`). El formato
final de los demás nombres lo tiene que definir el grupo del CU-01.

## 8. Registro de cambios (revisión 29/oct)

| # | Cambio | Motivo |
|---|---|---|
| 1 | `causaTipo`: se quitan `EMERGENCIA` y `VIAJE`; se agrega `EVENTO_PUBLICO` | Ninguna métrica usa los dos primeros (el caso 5 va por `correlationId`). `EVENTO_PUBLICO` separa el caso 3 (evento masivo, un `eventoId`) del caso 6 (reserva ciudadana, un `reservaId`); antes los dos usaban `RESERVA` y no quedaba claro qué id iba en `causaId`. |
| 2 | Nuevo `EventoPublicoCreado/Actualizado` (Espacios) | La creación de un evento masivo no se publicaba: los demás módulos no podían reaccionar al caso 3. También resuelve el caso 1 de Espacios ("eventos culturales cancelados") sin contar inscripciones. |
| 3 | Nuevo `EspacioPublicoAfectado/Restaurado` (Espacios) | Casos 2 y 4 cuentan *espacios afectados*; contarlos por `ReservaActualizada` dejaba afuera los espacios sin reservas. |
| 4 | `ReservaActualizada` ya no lleva `causaId`/`causaTipo` | Sus usos pasaron a los eventos de los puntos 2 y 3. |
| 5 | `ReservaCreada.zona` → `latitud`/`longitud`: confirmado | Necesario para el caso 6 (ubicar el espacio reservado). |
| 6 | Emergencias: `prioridadOriginal` en `EmergenciaCreada` | La métrica del caso 1 cambió a "criticidad afectada por evento climático". |
| 7 | Emergencias: categoría `PREVENTIVA`; caso 3 deja de filtrar por `EVENTO_CLIMATICO` | La métrica es "enviados al evento de manera preventiva"; incluir el clima mezclaba casos. |
| 8 | Residuos: se reemplaza `RecoleccionProgramada/Actualizada` por `IntervencionProgramada` (extendido) + `IntervencionActualizada` | Residuos ya publica `IntervencionProgramada` para el CU-01; dos eventos de planificación eran redundantes. Se agregan `camionId` (cuadrillas), `alertaId` (base del caso 5) y la causa. |
| 9 | Residuos: la causa de la afectación va en `IntervencionActualizada` | Antes solo estaba en la creación; una intervención normal afectada por clima no se podía contar. |
| 10 | Residuos c2 "por barrio": barrio desde `ReclamoCreado` vía `causaId` | Los eventos nuevos no llevan barrio. |
| 11 | `IncidenteVialCreado`: `estado` inicial e `inicioPrevisto`; `IncidenteVialActualizado`: `correlationId`, estado `CANCELADO` | Las métricas del caso 4 piden incidentes *programados*. |
| 12 | `EstacionUrbanaAfectada`: `afectacionDesde`/`afectacionHastaEstimada`, `correlationId` | Caso 3 pide estaciones "que se sabe que serán afectadas" (afectación anunciada a futuro). |
| 13 | `AlertaClimaticaGenerada`: `vigenteDesde`/`vigenteHastaEstimado` | Los módulos necesitan la ventana para decidir si algo es "a causa" de la alerta. |
| 14 | Se corrigen referencias internas (§5/§6) y se agrega la matriz §4.6 | Integridad del documento. |
