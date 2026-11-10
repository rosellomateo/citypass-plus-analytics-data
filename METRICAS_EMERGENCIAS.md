# Métricas de Emergencias — condiciones para contabilizar

| Caso | Métrica | Condición |
|---|---|---|
| 1 | Criticidad afectada por evento climático | `causaTipo = EVENTO_CLIMATICO`, y dentro de esas, `prioridad != prioridadOriginal` |
| 2 | Emergencias a causa de un reclamo | `causaTipo = RECLAMO` |
| 3 | Servicios enviados preventivamente | `categoria = PREVENTIVA` |
| 4 | Emergencias por incidentes de tránsito | `afectaCirculacion = true` |
| 5 | Emergencias por incidente vial | `causaTipo = INCIDENTE_VIAL` |
| 6 | Emergencias por evento de ciudadanos | `causaTipo = RESERVA` |
