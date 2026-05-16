# Pipeline Big Data IoT con Arquitectura Medallon

Proyecto final de Big Data para capturar, validar, procesar y visualizar datos de un unico dispositivo IoT (`sensor_aula_01`) usando una arquitectura Medallon.

## Flujo

```text
Arduino / simulador
  -> ingesta
  -> Raw CSV
  -> Bronze HDFS
  -> calidad y cuarentena
  -> Silver MinIO Parquet
  -> Gold MinIO Parquet
  -> Superset
```

## Componentes

- Arduino con sensor DHT11 para capturar temperatura y humedad.
- Python para ingesta serie/HTTP y generacion fallback.
- HDFS para capa Bronze y cuarentena.
- Spark/PySpark para validacion, limpieza y transformaciones.
- MinIO como almacenamiento S3 para Silver y Gold.
- Airflow para orquestar el pipeline completo.
- Superset para el dashboard final.

## Estructura principal

```text
src/
  airflow/dags/iot_medallion_pipeline.py
  jobs/
    ingestion/
    quality/
    medallion/
    superset/
  quality/rules_iot.yaml
requirements.txt
data/iot/
  inbox/
  raw/
  reports/
  superset/
docs/
esp32/
configs/
```

Resumen de carpetas:

- `data/iot/inbox/`: eventos recibidos antes de preparar el lote raw.
- `data/iot/raw/`: CSV bruto usado como entrada del pipeline.
- `data/iot/reports/`: informes JSON de validacion, calidad, Silver y Gold.
- `data/iot/superset/`: SQL y conexion DuckDB para crear visualizaciones en Superset.
- `src/jobs/ingestion/`: captura Arduino/HTTP, generador fallback y preparacion del lote.
- `src/jobs/quality/`: validaciones de raw, Bronze y Silver.
- `src/jobs/medallion/`: carga Bronze, transformacion a Silver y construccion de Gold.
- `src/jobs/superset/`: publicacion de consultas para Superset.
- `src/airflow/dags/`: DAG que orquesta el pipeline completo.
- `src/quality/`: reglas de calidad en YAML.
- `configs/hadoop/`: configuracion Hadoop/HDFS/YARN.
- `docs/`: arquitectura, ejecucion, diccionario de datos y guia de exposicion.
- `esp32/`: sketch del dispositivo IoT.
- `scripts/`: script auxiliar de arranque Hadoop.
- `requirements.txt`: dependencias Python usadas por los scripts y documentacion para ejecucion local.

## Dependencias Python

El proyecto esta preparado para ejecutarse con Docker Compose. Las imagenes ya instalan las dependencias necesarias, pero se incluye `requirements.txt` para documentarlas y para ejecucion local si hiciera falta:

```bash
python -m pip install -r requirements.txt
```

Para el pipeline completo tambien se necesita el entorno de servicios definido en `docker-compose.yml`: Hadoop/HDFS, Spark, MinIO, Airflow y Superset.

## Arranque

Crear el `.env` local a partir de la plantilla:

```bash
cp .env.example .env
```

En Windows PowerShell:

```powershell
Copy-Item .env.example .env
```

Levantar el entorno:

```bash
docker compose --profile core --profile batch --profile orchestration --profile bi up -d
```

Servicios:

```text
JupyterLab:    http://localhost:8888
Airflow:       http://localhost:8081      admin / admin
MinIO:         http://localhost:9001      admin / adminadmin
Superset:      http://localhost:8089      admin / admin
HDFS Explorer: http://localhost:9870
YARN:          http://localhost:8088
IoT receiver:  http://localhost:5050/health
```

## Captura desde Arduino

Listar puertos:

```bash
python src/jobs/ingestion/collect_arduino_serial.py --list-ports
```

Capturar datos:

```bash
python src/jobs/ingestion/collect_arduino_serial.py --port COM7 --date 2026-05-16 --max-records 500
```

El capturador escribe en:

```text
data/iot/inbox/esp32_events_2026-05-16.csv
data/iot/raw/run_date=2026-05-16/iot_sensor_aula_01_2026-05-16.csv
```

## Ejecucion manual del pipeline

Dentro de Jupyter:

```bash
cd /home/jovyan/work
export PROJECT_ROOT=/home/jovyan/work
export PYTHONPATH=/usr/local/spark/python:/usr/local/spark/python/lib/py4j-0.10.9.7-src.zip:/usr/local/spark/python/lib/pyspark.zip:$PYTHONPATH
python src/jobs/run_iot_pipeline.py --date 2026-05-16 --min-real-records 10
```

Fases ejecutadas:

```text
collect_iot_batch
validate_raw
load_bronze_hdfs
quality_bronze
bronze_to_silver
validate_silver
silver_to_gold
publish_gold_superset
```

## Resultados de prueba

Ejecucion principal con datos reales y errores controlados:

```text
Fecha:              2026-05-16
Registros raw:      1894
Registros validos:  1694
Registros invalidos: 200
Nulos timestamp:    5
Valores fuera rango: 83
```

Informes:

```text
data/iot/reports/run_date=2026-05-16/
```

## Documentacion

- [Arquitectura](docs/arquitectura.md)
- [Diccionario de datos](docs/diccionario_datos.md)
- [Manual de ejecucion](docs/manual_ejecucion.md)
- [Guia de plataformas y exposicion](docs/guia_plataformas_y_exposicion.md)
- [Diagrama y ejecucion](docs/diagrama_y_ejecucion.md)
