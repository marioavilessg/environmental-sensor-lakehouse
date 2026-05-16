# Diccionario de datos IoT

## Evento raw ESP32

| Campo | Tipo esperado | Obligatorio | Descripcion |
| --- | --- | --- | --- |
| `event_id` | string | Si | Identificador unico del evento. |
| `device_id` | string | Si | Identificador del dispositivo; valor esperado `sensor_aula_01`. |
| `timestamp` | string ISO | Si | Momento de la lectura, formato `YYYY-MM-DDTHH:MM:SS`. |
| `temperature` | numeric | Si | Temperatura en grados Celsius. Rango valido: -20 a 80. |
| `humidity` | numeric | Si | Humedad relativa en porcentaje. Rango valido: 0 a 100. |
| `battery` | numeric | Si | Nivel de bateria en porcentaje. Rango valido: 0 a 100. |
| `status` | string | Si | Estado del dispositivo: `OK`, `WARN` o `ERROR`. |
| `source` | string | Si | Origen del dato: `esp32` o `simulator`. |

## Silver

Silver conserva solo registros validos. `timestamp` se convierte a `event_ts`, las metricas se tipan como numericas y se anaden particiones `year`, `month`, `day` y `hour`.

## Gold

- `hourly_metrics`: medias, maximos, minimos y eventos por hora.
- `events_by_hour`: conteo de eventos por hora.
- `anomalies_by_type`: conteo de incidencias por tipo y hora.
- `daily_quality`: resumen diario de calidad.
- `battery_hourly`: evolucion horaria de la bateria.

