# Arquitectura del pipeline IoT Medallon

## Caso de uso

El proyecto analiza un unico dispositivo IoT: `sensor_aula_01`, una placa ESP32 con sensor de temperatura y humedad. El objetivo es estudiar la evolucion temporal de sus mediciones, el estado del dispositivo y la calidad de los datos recibidos.

## Flujo de datos

1. ESP32 envia lecturas CSV por HTTP POST a `http://<IP_DEL_PC>:5050/iot/events`.
2. `iot-ingestor` guarda cada evento como CSV con cabecera en `data/iot/inbox/`.
3. Airflow ejecuta `iot_medallion_pipeline`.
4. Si hay suficientes datos reales, se prepara el lote ESP32; si no, se genera un lote simulador reproducible.
5. Bronze conserva el CSV raw en HDFS:
   `/datalake/bronze/iot/sensor_aula_01/year=YYYY/month=MM/day=DD/`
6. La fase de calidad separa validos e invalidos. Los invalidos se escriben en cuarentena HDFS:
   `/datalake/quarantine/iot/sensor_aula_01/year=YYYY/month=MM/day=DD/`
7. Spark genera Silver tipado y limpio en MinIO, formato Parquet:
   `s3a://datalake/silver/iot/sensor_aula_01/events/`
8. Spark genera Gold en MinIO:
   `hourly_metrics`, `events_by_hour`, `anomalies_by_type`, `daily_quality`, `battery_hourly`.
9. Superset consulta Gold directamente con DuckDB/httpfs.

## Componentes

- ESP32: dispositivo real de captura.
- `iot-ingestor`: API HTTP ligera para recibir eventos.
- HDFS: capa Bronze y cuarentena.
- Spark: validacion distribuida, Silver y Gold.
- MinIO: almacenamiento de Silver y Gold en Parquet.
- Airflow: orquestacion completa del pipeline.
- Superset: dashboard analitico final.
