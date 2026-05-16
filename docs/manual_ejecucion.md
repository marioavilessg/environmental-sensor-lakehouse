# Manual de ejecucion

Para una explicacion completa con diagrama del flujo, consultar tambien:

```text
docs/diagrama_y_ejecucion.md
```

## Arranque del entorno

Construir imagenes despues de los cambios. Como los servicios estan organizados por perfiles, hay que indicar los perfiles que contienen servicios con `build`:

```bash
docker compose --profile core --profile orchestration --profile bi build
```

Arrancar servicios para ejecutar el pipeline completo:

```bash
docker compose --profile core --profile batch --profile orchestration up -d
```

Para trabajar tambien con Superset:

```bash
docker compose --profile core --profile batch --profile orchestration --profile bi up -d
```

## Receptor HTTP para ESP32

El servicio `iot-ingestor` queda disponible en:

```text
http://localhost:5050/iot/events
```

Desde otro dispositivo de la red, sustituye `localhost` por la IP del PC.

Prueba manual:

```bash
curl -X POST http://localhost:5050/iot/events ^
  -H "Content-Type: text/csv" ^
  -d "manual_001,sensor_aula_01,2026-05-15T10:00:00,24.5,55.2,91,OK,esp32"
```

Verificar recepcion:

```bash
type data\iot\inbox\esp32_events_2026-05-15.csv
```

## Ejecutar el pipeline completo manualmente

Entrar en Jupyter:

```bash
docker exec -it jupyter-aula bash
cd /home/jovyan/work
export PROJECT_ROOT=/home/jovyan/work
export PYTHONPATH=/usr/local/spark/python:/usr/local/spark/python/lib/py4j-0.10.9.7-src.zip:/usr/local/spark/python/lib/pyspark.zip:$PYTHONPATH
```

Ejecutar todo en orden:

```bash
python src/jobs/run_iot_pipeline.py --date 2026-05-15 --min-real-records 10
```

Reanudar desde una fase concreta, por ejemplo si ya cargaste Bronze:

```bash
python src/jobs/run_iot_pipeline.py --date 2026-05-15 --start-at quality_bronze
```

## Ejecutar scripts por piezas

Si quieres depurar una fase concreta, puedes ejecutar los scripts por separado:

```bash
python src/jobs/ingestion/collect_iot_batch.py --date 2026-05-15 --min-real-records 10
python src/jobs/quality/validate_raw.py --date 2026-05-15
python src/jobs/medallion/load_bronze_hdfs.py --date 2026-05-15
python src/jobs/quality/quality_bronze.py --date 2026-05-15
python src/jobs/medallion/bronze_to_silver.py --date 2026-05-15
python src/jobs/quality/validate_silver.py --date 2026-05-15
python src/jobs/medallion/silver_to_gold.py --date 2026-05-15
python src/jobs/superset/publish_gold_superset.py --date 2026-05-15
```

## Ejecutar desde Airflow

1. Abrir `http://localhost:8081`.
2. Entrar con `admin / admin`.
3. Activar y lanzar el DAG `iot_medallion_pipeline`.
4. Comprobar que todas las tareas terminan en `success`.

## Evidencias recomendadas

- HDFS Explorer con Bronze:
  `/datalake/bronze/iot/sensor_aula_01/`
- HDFS Explorer con cuarentena:
  `/datalake/quarantine/iot/sensor_aula_01/`
- MinIO con Silver y Gold dentro del bucket `datalake`.
- Airflow Graph View con el DAG completo en verde.
- Ficheros JSON de informes en `data/iot/reports/run_date=YYYY-MM-DD/`.
- Superset con graficas creadas desde las SQL de `data/iot/superset/`.

## Superset

Crear una conexion DuckDB:

```text
duckdb:////tmp/superset_lakehouse.db
```

En `Advanced > Other > Engine Parameters`, pegar el JSON generado en:

```text
data/iot/superset/duckdb_connection.md
```

Visualizaciones minimas:

- Line chart: `avg_temperature` y `avg_humidity` por `date_hour`.
- Bar chart: `num_events` por `date_hour`.
- Bar chart: `total_anomalies` por `anomaly_type`.
- KPI opcional: `valid_percentage` desde `daily_quality`.
