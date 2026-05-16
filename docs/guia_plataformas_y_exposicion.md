# Guia de plataformas web y exposicion del proyecto IoT Medallon

Este documento explica que papel tiene cada herramienta del entorno, que debes comprobar en cada interfaz web y como contar el flujo completo durante la exposicion.

Fecha usada en la prueba principal:

```text
2026-05-16
```

Resultados de referencia de la ejecucion manual:

```text
Raw:                1894 registros
Registros validos:  1694
Registros invalidos: 200
Nulos timestamp:    5
Fuera de rango:     83
Silver:             1694 registros limpios
Gold:               5 datasets analiticos
```

## 1. Idea general del proyecto

El proyecto implementa un pipeline Big Data con arquitectura Medallon para datos IoT de un unico dispositivo:

```text
sensor_aula_01
```

El sensor representa un aula y recoge:

- temperatura;
- humedad;
- bateria;
- estado del dispositivo;
- momento de la lectura.

El objetivo no es comparar varios sensores, sino analizar la evolucion temporal de un unico dispositivo y comprobar la calidad de los datos recibidos.

## 2. Flujo completo del dato

El flujo que debes explicar es este:

```text
Arduino / simulador
  -> ingesta local
  -> raw CSV
  -> Bronze HDFS
  -> validacion de calidad
  -> cuarentena HDFS
  -> Silver MinIO Parquet
  -> Gold MinIO Parquet
  -> Superset
```

Explicacion corta:

1. El Arduino genera lecturas de temperatura y humedad.
2. Python captura esas lecturas y las guarda en CSV raw.
3. El pipeline carga ese CSV bruto en HDFS como capa Bronze.
4. Spark valida tipos, nulos, rangos, fechas y duplicados.
5. Los registros malos se separan en cuarentena.
6. Los registros buenos se guardan limpios en MinIO como Silver.
7. Desde Silver se generan agregaciones Gold.
8. Superset consulta Gold para construir el dashboard.

## 3. JupyterLab

URL:

```text
http://localhost:8888
```

Para que sirve:

JupyterLab es el entorno de trabajo donde se ejecutan los scripts manualmente. Sirve para probar el pipeline fase a fase antes de lanzarlo con Airflow.

Que debes mostrar:

- La estructura del proyecto.
- La carpeta `data/iot/raw/run_date=2026-05-16/`.
- El fichero raw:

```text
data/iot/raw/run_date=2026-05-16/iot_sensor_aula_01_2026-05-16.csv
```

- La carpeta de informes:

```text
data/iot/reports/run_date=2026-05-16/
```

Comando principal ejecutado:

```bash
cd /home/jovyan/work
export PROJECT_ROOT=/home/jovyan/work
export PYTHONPATH=/usr/local/spark/python:/usr/local/spark/python/lib/py4j-0.10.9.7-src.zip:/usr/local/spark/python/lib/pyspark.zip:$PYTHONPATH
python src/jobs/run_iot_pipeline.py --date 2026-05-16 --min-real-records 10
```

Frase para exponer:

> Primero pruebo el pipeline desde Jupyter para comprobar que cada script funciona de forma independiente antes de orquestarlo con Airflow.

## 4. HDFS Explorer

URL:

```text
http://localhost:9870
```

Para que sirve:

HDFS almacena la capa Bronze y la cuarentena. Bronze guarda el dato bruto, lo mas parecido posible al dato original recibido.

Que debes comprobar:

Bronze:

```text
/datalake/bronze/iot/sensor_aula_01/year=2026/month=05/day=16/
```

Debe aparecer:

```text
iot_sensor_aula_01_2026-05-16.csv
```

Cuarentena:

```text
/datalake/quarantine/iot/sensor_aula_01/year=2026/month=05/day=16/
```

Debe aparecer un fichero JSON con los registros invalidos.

Comandos equivalentes:

```bash
hdfs dfs -ls /datalake/bronze/iot/sensor_aula_01/year=2026/month=05/day=16
hdfs dfs -ls /datalake/quarantine/iot/sensor_aula_01/year=2026/month=05/day=16
```

Evidencia ya comprobada:

```text
Bronze contiene 1 CSV raw.
Cuarentena contiene 1 JSON con registros invalidos.
```

Frase para exponer:

> HDFS conserva el dato bruto en Bronze. Si una transformacion falla o cambian las reglas de calidad, puedo volver a procesar desde el origen.

## 5. MinIO

URL:

```text
http://localhost:9001
```

Credenciales:

```text
usuario: admin
password: adminadmin
```

Para que sirve:

MinIO simula un almacenamiento de objetos tipo S3. En este proyecto contiene las capas Silver y Gold en formato Parquet.

Que debes comprobar:

Bucket:

```text
datalake
```

Silver:

```text
silver/iot/sensor_aula_01/events/
```

Gold:

```text
gold/iot/sensor_aula_01/hourly_metrics/
gold/iot/sensor_aula_01/events_by_hour/
gold/iot/sensor_aula_01/anomalies_by_type/
gold/iot/sensor_aula_01/battery_hourly/
gold/iot/sensor_aula_01/daily_quality/
```

Que significa cada dataset Gold:

- `hourly_metrics`: temperatura, humedad y bateria agregadas por hora.
- `events_by_hour`: numero de eventos por hora.
- `anomalies_by_type`: conteo de anomalías por tipo.
- `battery_hourly`: evolucion horaria de bateria.
- `daily_quality`: resumen diario de calidad del dato.

Frase para exponer:

> MinIO almacena los datos ya preparados para analisis. Silver contiene registros limpios y Gold contiene datasets agregados para visualizacion.

## 6. Airflow

URL:

```text
http://localhost:8081
```

Credenciales:

```text
usuario: admin
password: admin
```

Para que sirve:

Airflow orquesta el pipeline completo. En lugar de ejecutar scripts a mano, el DAG lanza cada fase en orden y controla dependencias, logs y reintentos.

DAG:

```text
iot_medallion_pipeline
```

Tareas del DAG:

```text
check_or_collect_esp32_data
validate_raw
load_bronze_hdfs
quality_bronze
bronze_to_silver
validate_silver
silver_to_gold
publish_gold_superset
```

Que debes hacer:

1. Entrar en Airflow.
2. Buscar `iot_medallion_pipeline`.
3. Activar el DAG.
4. Pulsar `Trigger DAG`.
5. Abrir `Graph` o `Grid`.
6. Comprobar que todas las tareas terminan en verde.
7. Sacar captura del DAG completo.

Frase para exponer:

> Airflow demuestra que el pipeline no es una ejecucion manual aislada, sino un proceso orquestado y repetible.

## 7. Superset

URL:

```text
http://localhost:8089
```

Credenciales:

```text
usuario: admin
password: admin
```

Para que sirve:

Superset es la capa final de visualizacion. Usa los datasets Gold para crear graficas sobre el comportamiento temporal del sensor y la calidad del dato.

Conexion recomendada:

```text
duckdb:////tmp/superset_lakehouse.db
```

Parametros avanzados:

Estan en:

```text
data/iot/superset/duckdb_connection.md
```

SQL disponibles:

```text
data/iot/superset/01_hourly_metrics.sql
data/iot/superset/02_anomalies_by_type.sql
data/iot/superset/03_daily_quality.sql
data/iot/superset/04_battery_hourly.sql
data/iot/superset/05_events_by_hour.sql
```

Graficas minimas recomendadas:

1. Line chart:

```text
avg_temperature y avg_humidity por date_hour
```

2. Bar chart:

```text
num_events por date_hour
```

3. Bar chart:

```text
total_anomalies por anomaly_type
```

4. KPI opcional:

```text
valid_percentage desde daily_quality
```

Frase para exponer:

> Superset no trabaja sobre el raw, sino sobre Gold. Asi el dashboard consulta datos ya agregados, limpios y preparados para analisis.

## 8. YARN

URL:

```text
http://localhost:8088
```

Para que sirve:

YARN es el gestor de recursos del ecosistema Hadoop. En este proyecto se puede usar como evidencia del entorno Big Data, aunque las ejecuciones Spark se lanzan en modo local dentro del contenedor.

Que puedes mostrar:

- Que el servicio esta levantado.
- Que forma parte del entorno Hadoop.

Frase para exponer:

> YARN forma parte del entorno Hadoop disponible, aunque en este proyecto las transformaciones Spark se ejecutan en modo local para simplificar la practica.

## 9. Informes de calidad

Ruta:

```text
data/iot/reports/run_date=2026-05-16/
```

Ficheros importantes:

```text
validate_raw_report.json
quality_bronze_report.json
bronze_to_silver_report.json
validate_silver_report.json
silver_to_gold_report.json
```

Datos clave para explicar:

```text
total_records: 1894
valid_records: 1694
invalid_records: 200
duplicates: 0
null_timestamps: 5
out_of_range_values: 83
```

Interpretacion:

- El CSV tiene estructura correcta.
- Hay 200 registros invalidos introducidos de forma controlada.
- Esos registros no se mezclan con Silver.
- Silver queda con 1694 registros validos.
- Gold se genera desde los datos limpios.

Frase para exponer:

> La calidad no se limita a borrar datos malos. El pipeline los identifica, los cuenta y los separa en cuarentena para poder auditarlos.

## 10. Que capturas incluir en el PDF

Capturas minimas:

1. Raw CSV en Jupyter o VS Code.
2. Bronze en HDFS Explorer.
3. Cuarentena en HDFS Explorer.
4. Silver en MinIO.
5. Gold en MinIO.
6. Informe `quality_bronze_report.json`.
7. DAG de Airflow en verde.
8. Dashboard de Superset.

Capturas recomendadas extra:

- Terminal con `Pipeline completed successfully`.
- SQL de Superset en `data/iot/superset/`.
- Vista de servicios Docker levantados.

## 11. Guion corto para la exposicion

Puedes explicarlo asi:

> Mi proyecto implementa una arquitectura Medallon para datos IoT de un unico sensor ambiental llamado `sensor_aula_01`.
>
> Primero capturo datos reales desde Arduino y los guardo en formato raw. Tambien introduzco errores controlados, como campos vacios, valores fuera de rango y texto en columnas numericas, para poder demostrar la fase de calidad.
>
> Despues el pipeline carga el dato bruto en HDFS como capa Bronze. Esta capa no transforma el dato, solo lo conserva.
>
> A continuacion Spark aplica reglas de calidad: comprueba columnas obligatorias, nulos, tipos, rangos, fechas y duplicados. Los registros invalidos se guardan en cuarentena y se genera un informe JSON.
>
> Los registros validos pasan a Silver en MinIO, en formato Parquet y con tipos correctos. Desde Silver genero datasets Gold agregados por hora y por tipo de incidencia.
>
> Finalmente Airflow orquesta todo el proceso con un DAG y Superset visualiza los resultados desde la capa Gold.

## 12. Orden recomendado para probar en directo

1. Jupyter:

```bash
python src/jobs/run_iot_pipeline.py --date 2026-05-16 --min-real-records 10
```

2. HDFS:

```text
Comprobar Bronze y cuarentena.
```

3. MinIO:

```text
Comprobar Silver y Gold en bucket datalake.
```

4. Airflow:

```text
Lanzar DAG y revisar tareas en verde.
```

5. Superset:

```text
Mostrar dashboard final.
```

