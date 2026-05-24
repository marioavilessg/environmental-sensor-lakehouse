# Preparacion para la presentacion del proyecto IoT Medallion

Este documento es una chuleta de exposicion. Esta pensado para responder rapido a:

- cual es el flujo completo del dato;
- donde se ve cada capa en Hadoop, MinIO, Airflow y Superset;
- que significa cada fase del pipeline;
- que preguntas pueden hacerte y como responderlas.

Fecha de referencia de la ejecucion preparada:

```text
2026-05-16
```

Dispositivo analizado:

```text
sensor_aula_01
```

## 1. Resumen en 30 segundos

Frase de apertura:

> Este proyecto implementa un pipeline Big Data para datos IoT de un sensor ambiental ESP32. El flujo sigue una arquitectura Medallion: primero guardo el dato bruto en Bronze sobre HDFS, despues aplico reglas de calidad con Spark, separo registros invalidos en cuarentena, guardo los registros limpios en Silver sobre MinIO en Parquet, genero agregados Gold y finalmente visualizo esos datasets en Superset. Airflow orquesta todo el proceso para que sea repetible.

Idea clave:

```text
ESP32 / simulador
  -> ingesta local
  -> Raw CSV
  -> Bronze HDFS
  -> calidad Spark + cuarentena HDFS
  -> Silver MinIO Parquet
  -> Gold MinIO Parquet
  -> Superset
```

Diagrama visual:

```text
docs/diagrama_pipeline_grande.svg
```

## 2. Mapa rapido de entornos

| Entorno | URL | Credenciales | Que ensenar |
| --- | --- | --- | --- |
| JupyterLab | `http://localhost:8888` | sin token | ejecucion manual, scripts, CSV raw e informes JSON |
| Hadoop HDFS | `http://localhost:9870` | no aplica | Bronze y cuarentena |
| Hadoop YARN | `http://localhost:8088` | no aplica | ResourceManager levantado como parte del entorno Hadoop |
| MinIO | `http://localhost:9001` | `admin / adminadmin` | bucket `datalake`, Silver y Gold |
| Airflow | `http://localhost:8081` | `admin / admin` | DAG `iot_medallion_pipeline` y tareas en verde |
| Superset | `http://localhost:8089` | `admin / admin` | graficas creadas desde datasets Gold |
| Receptor IoT | `http://localhost:5050/health` | no aplica | API HTTP que recibe eventos ESP32 |

Comando para levantarlo todo:

```bash
docker compose --profile core --profile batch --profile orchestration --profile bi up -d
```

## 3. Flujo completo por fases

| Orden | Fase | Script / tarea | Entrada | Salida | Donde se ve |
| --- | --- | --- | --- | --- | --- |
| 1 | Captura o fallback | `collect_iot_batch.py` | `data/iot/inbox/esp32_events_2026-05-16.csv` o simulador | CSV raw | Jupyter / carpeta local |
| 2 | Validacion raw | `validate_raw.py` | CSV raw | `validate_raw_report.json` | Jupyter / informes |
| 3 | Carga Bronze | `load_bronze_hdfs.py` | CSV raw | Bronze en HDFS | HDFS Explorer |
| 4 | Calidad Bronze | `quality_bronze.py` | Bronze HDFS | informe de calidad + cuarentena | HDFS + informes |
| 5 | Bronze a Silver | `bronze_to_silver.py` | Bronze validado | Silver Parquet | MinIO |
| 6 | Validacion Silver | `validate_silver.py` | Silver Parquet | `validate_silver_report.json` | informes |
| 7 | Silver a Gold | `silver_to_gold.py` | Silver limpio | 5 datasets Gold | MinIO |
| 8 | Publicacion BI | `publish_gold_superset.py` | rutas Gold | SQL para Superset | Superset / carpeta SQL |

Comando manual equivalente al DAG:

```bash
cd /home/jovyan/work
export PROJECT_ROOT=/home/jovyan/work
export PYTHONPATH=/usr/local/spark/python:/usr/local/spark/python/lib/py4j-0.10.9.7-src.zip:/usr/local/spark/python/lib/pyspark.zip:$PYTHONPATH
python src/jobs/run_iot_pipeline.py --date 2026-05-16 --min-real-records 10
```

## 4. Que se ve en Hadoop

Hadoop se usa en dos partes:

- HDFS: almacenamiento distribuido para Bronze y cuarentena.
- YARN: gestor de recursos disponible en el entorno Hadoop.

### HDFS NameNode

URL:

```text
http://localhost:9870
```

Que mostrar:

1. Entrar en `Utilities > Browse the file system`.
2. Buscar la capa Bronze:

```text
/datalake/bronze/iot/sensor_aula_01/year=2026/month=05/day=16/
```

Debe aparecer:

```text
iot_sensor_aula_01_2026-05-16.csv
```

3. Buscar la cuarentena:

```text
/datalake/quarantine/iot/sensor_aula_01/year=2026/month=05/day=16/
```

Debe aparecer un fichero JSON generado por Spark con registros invalidos.

Frase para defenderlo:

> Uso HDFS para conservar el dato bruto en Bronze y los errores en cuarentena. Asi puedo auditar los datos malos y reprocesar el pipeline si cambian las reglas.

Comandos equivalentes:

```bash
hdfs dfs -ls /datalake/bronze/iot/sensor_aula_01/year=2026/month=05/day=16
hdfs dfs -ls /datalake/quarantine/iot/sensor_aula_01/year=2026/month=05/day=16
```

### YARN ResourceManager

URL:

```text
http://localhost:8088
```

Que mostrar:

- que el ResourceManager esta levantado;
- que forma parte del ecosistema Hadoop del proyecto.

Matiz importante:

> En esta practica las transformaciones Spark se lanzan en modo local dentro del contenedor de Jupyter para simplificar la ejecucion. YARN esta disponible como parte del entorno Hadoop, pero el pipeline no depende de ejecutar Spark sobre YARN.

## 5. Que se ve en MinIO

URL:

```text
http://localhost:9001
```

Credenciales:

```text
admin / adminadmin
```

Bucket:

```text
datalake
```

### Silver

Ruta:

```text
silver/iot/sensor_aula_01/events/
```

Que significa:

> Silver contiene solo eventos validos, tipados y en formato Parquet. Aqui el timestamp ya se convierte a `event_ts`, las metricas pasan a numericas y se anaden particiones temporales.

Datos de la ejecucion:

```text
Registros escritos en Silver: 1694
Registros invalidos descartados: 200
```

### Gold

Rutas:

```text
gold/iot/sensor_aula_01/hourly_metrics/
gold/iot/sensor_aula_01/events_by_hour/
gold/iot/sensor_aula_01/anomalies_by_type/
gold/iot/sensor_aula_01/battery_hourly/
gold/iot/sensor_aula_01/daily_quality/
```

Que significa cada dataset:

| Dataset Gold | Para que sirve | Registros de referencia |
| --- | --- | --- |
| `hourly_metrics` | medias, minimos y maximos de temperatura, humedad y bateria por hora | 3 |
| `events_by_hour` | numero de eventos por hora | 3 |
| `anomalies_by_type` | incidencias por tipo, por ejemplo temperatura alta o bateria baja | 3 |
| `battery_hourly` | evolucion horaria de la bateria | 3 |
| `daily_quality` | resumen de calidad del dia | 1 |

Frase para defenderlo:

> MinIO simula un almacenamiento S3 moderno. Uso Parquet porque es columnar, eficiente para analitica y adecuado para que Superset lea datasets preparados.

## 6. Que se ve en Airflow

URL:

```text
http://localhost:8081
```

Credenciales:

```text
admin / admin
```

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

Que mostrar:

1. Activar el DAG.
2. Pulsar `Trigger DAG`.
3. Ir a `Graph` o `Grid`.
4. Abrir algun log para demostrar que ejecuta comandos reales dentro de `jupyter-aula`.
5. Ensenar todas las tareas en verde.

Como explicarlo:

> Airflow no transforma datos por si mismo. Su funcion aqui es coordinar las fases, controlar el orden, guardar logs y permitir reintentos. Cada nodo del DAG lanza un script Python del pipeline.

Detalle tecnico importante:

El DAG usa `BashOperator` y ejecuta los scripts dentro del contenedor `jupyter-aula` mediante `docker exec`. Eso permite que Airflow orqueste y que Jupyter tenga las dependencias de Spark/HDFS.

## 7. Que se ve en Superset

URL:

```text
http://localhost:8089
```

Credenciales:

```text
admin / admin
```

Conexion:

```text
duckdb:////tmp/superset_lakehouse.db
```

Parametros avanzados:

```text
data/iot/superset/duckdb_connection.md
```

SQL preparados:

```text
data/iot/superset/01_hourly_metrics.sql
data/iot/superset/02_anomalies_by_type.sql
data/iot/superset/03_daily_quality.sql
data/iot/superset/04_battery_hourly.sql
data/iot/superset/05_events_by_hour.sql
```

Graficas que debes tener o crear:

| Grafica | SQL / dataset | Tipo recomendado | Que explica |
| --- | --- | --- | --- |
| Temperatura y humedad por hora | `01_hourly_metrics.sql` | Line chart | evolucion ambiental |
| Eventos por hora | `05_events_by_hour.sql` | Bar chart | volumen de lecturas |
| Anomalias por tipo | `02_anomalies_by_type.sql` | Bar chart | incidencias detectadas |
| Calidad diaria | `03_daily_quality.sql` | KPI / Table | porcentaje valido e invalidos |
| Bateria por hora | `04_battery_hourly.sql` | Line chart | estado del dispositivo |

Frase para defenderlo:

> Superset consulta Gold, no Raw ni Bronze. Esto evita que el dashboard haga limpieza o calculos pesados en cada visualizacion.

## 8. Resultados clave para memorizar

Ejecucion del 16 de mayo de 2026:

```text
Raw total:             1894 registros
Registros validos:     1694
Registros invalidos:   200
Duplicados:            0
Timestamps nulos:      5
Valores fuera de rango:83
Silver:                1694 registros limpios
Gold:                  5 datasets analiticos
```

Informe principal:

```text
data/iot/reports/run_date=2026-05-16/quality_bronze_report.json
```

Frase de calidad:

> El pipeline no se limita a borrar datos malos. Los identifica, explica el motivo en `error_reason`, los separa en cuarentena y deja trazabilidad en informes JSON.

## 9. Guion recomendado de 5 a 7 minutos

### Minuto 1: problema y arquitectura

> El proyecto parte de un caso IoT: un ESP32 mide temperatura, humedad, bateria y estado en un aula. Como en un entorno real pueden llegar datos incompletos o incorrectos, construyo un pipeline Medallion para separar dato bruto, dato limpio y dato analitico.

### Minuto 2: ingesta y Raw

> La ingesta puede venir del ESP32 por HTTP o de un simulador reproducible si no hay suficientes datos reales. El resultado inicial es un CSV raw en `data/iot/raw/run_date=2026-05-16/`.

### Minuto 3: Hadoop, Bronze y cuarentena

> Ese CSV se carga en HDFS como Bronze, sin transformarlo. Despues Spark aplica reglas de calidad: columnas obligatorias, tipos numericos, rangos, timestamp valido, estado permitido y duplicados. Los invalidos se escriben en cuarentena HDFS.

### Minuto 4: MinIO, Silver y Gold

> Los registros validos pasan a Silver en MinIO como Parquet. Desde Silver genero Gold: metricas por hora, eventos por hora, anomalias, bateria y calidad diaria.

### Minuto 5: Airflow

> Airflow orquesta todo: cada tarea corresponde a una fase del pipeline. En la vista Graph o Grid puedo ver si cada fase ha terminado correctamente y revisar logs.

### Minuto 6: Superset

> Superset se conecta a los Parquet Gold mediante DuckDB y httpfs. Las graficas finales trabajan sobre datos limpios y agregados, por eso el dashboard es mas simple y eficiente.

### Cierre

> La idea principal es separar responsabilidades: HDFS conserva el bruto y la cuarentena, Spark valida y transforma, MinIO almacena Silver y Gold, Airflow orquesta y Superset visualiza.

## 10. Recorrido recomendado para la demo

La idea es no ensenar herramientas sueltas. Cada pantalla debe responder a una pregunta:

```text
Donde esta el dato ahora?
Que cambio respecto a la fase anterior?
Que evidencia demuestra que ha funcionado?
```

### 10.1 Preparacion de pestanas

Antes de empezar, deja abiertas estas pestanas en este orden:

| Orden | Pantalla | Que tener abierto |
| --- | --- | --- |
| 1 | VS Code / Markdown | `docs/preparacion_presentacion.md` |
| 2 | VS Code / CSV | `data/iot/raw/run_date=2026-05-16/iot_sensor_aula_01_2026-05-16.csv` |
| 3 | Airflow | `http://localhost:8081`, DAG `iot_medallion_pipeline` en `Grid` o `Graph` |
| 4 | HDFS Bronze | `/datalake/bronze/iot/sensor_aula_01/year=2026/month=05/day=16/` |
| 5 | HDFS cuarentena | `/datalake/quarantine/iot/sensor_aula_01/year=2026/month=05/day=16/` |
| 6 | VS Code / informe | `data/iot/reports/run_date=2026-05-16/quality_bronze_report.json` |
| 7 | MinIO Silver | bucket `datalake`, ruta `silver/iot/sensor_aula_01/events/` |
| 8 | MinIO Gold | bucket `datalake`, ruta `gold/iot/sensor_aula_01/` |
| 9 | Superset | dashboard o SQL Lab con las graficas finales |

### 10.2 Inicio: flujo general

Pantalla:

```text
docs/preparacion_presentacion.md
```

Tambien puedes abrir el diagrama:

```text
docs/diagrama_pipeline_grande.svg
```

Que explicar:

> Antes de ensenar herramientas, voy a seguir el recorrido del dato. El dato nace en el ESP32 o simulador, se guarda como CSV raw, pasa a Bronze en HDFS, se valida con Spark, los errores van a cuarentena, los registros buenos pasan a Silver en MinIO, desde ahi genero Gold y finalmente Superset visualiza Gold.

No te quedes mucho aqui. Es solo el mapa mental.

### 10.3 Raw: punto de partida del dato

Pantalla:

```text
data/iot/raw/run_date=2026-05-16/iot_sensor_aula_01_2026-05-16.csv
```

Que ensenar:

- cabecera del CSV;
- algunas filas;
- columnas `event_id`, `device_id`, `timestamp`, `temperature`, `humidity`, `battery`, `status`, `source`.

Que explicar:

> Este es el dato raw de entrada. Todavia no esta limpio ni agregado. Es el lote que se va a procesar para la fecha `2026-05-16`.

Frase util:

> En esta fase me interesa conservar el dato tal como llega, porque todavia no se si es valido o invalido.

### 10.4 Airflow: orquestacion del pipeline

Pantalla:

```text
http://localhost:8081
```

Vista recomendada:

```text
DAG iot_medallion_pipeline -> Grid o Graph
```

Que ensenar:

- el DAG activado;
- las 8 tareas;
- todas las tareas en verde;
- si te da tiempo, abrir un log de una tarea para que se vea que ejecuta un script Python.

Que explicar:

> Airflow coordina el orden. No limpia ni transforma datos por si mismo; cada nodo lanza un script Python dentro del contenedor `jupyter-aula`.

Tareas que debes nombrar rapido:

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

Matiz importante:

> Aunque Airflow tenga una fecha interna de ejecucion, el DAG esta preparado para procesar `2026-05-16` por defecto. Si se pasa `run_date` en la configuracion del trigger, usa esa otra fecha.

### 10.5 HDFS Bronze: dato bruto conservado

Pantalla:

```text
http://localhost:9870
```

Ruta:

```text
/datalake/bronze/iot/sensor_aula_01/year=2026/month=05/day=16/
```

Que ensenar:

```text
iot_sensor_aula_01_2026-05-16.csv
```

Que explicar:

> Esta es la capa Bronze. Aqui guardo el CSV bruto en HDFS, sin transformarlo. Sirve para conservar el origen y poder reprocesar si cambian las reglas de calidad.

Frase corta:

> Bronze responde a la pregunta: donde esta guardado el dato original dentro del data lake?

### 10.6 HDFS cuarentena: registros invalidos auditables

Pantalla:

```text
http://localhost:9870
```

Ruta:

```text
/datalake/quarantine/iot/sensor_aula_01/year=2026/month=05/day=16/
```

Que ensenar:

- fichero `part-...json`;
- `_SUCCESS` si aparece.

Que explicar:

> Los registros malos no se borran sin mas. Se separan en cuarentena para poder auditarlos. Cada registro invalido conserva informacion de por que ha fallado mediante `error_reason`.

Frase corta:

> Cuarentena responde a la pregunta: que datos he rechazado y por que?

### 10.7 Informe de calidad: numeros de la ejecucion

Pantalla:

```text
data/iot/reports/run_date=2026-05-16/quality_bronze_report.json
```

Que ensenar:

```text
total_records: 1894
valid_records: 1694
invalid_records: 200
duplicates: 0
null_timestamps: 5
out_of_range_values: 83
```

Que explicar:

> Este informe es la trazabilidad de calidad. El pipeline no solo genera datos finales, tambien deja evidencia de cuantos registros ha procesado y cuantos ha rechazado.

Frase corta:

> Aqui demuestro que la calidad no es una explicacion teorica, sino una salida real del pipeline.

### 10.8 MinIO Silver: dato limpio en Parquet

Pantalla:

```text
http://localhost:9001
```

Ruta:

```text
datalake/silver/iot/sensor_aula_01/events/
```

Que ensenar:

- carpeta `events`;
- particiones `year`, `month`, `day`;
- fichero Parquet.

Que explicar:

> Silver contiene solo registros validos, tipados y guardados en Parquet. Aqui el dato ya esta listo para ser usado por procesos analiticos.

Frase corta:

> Silver responde a la pregunta: donde esta el dato limpio?

### 10.9 MinIO Gold: datasets para analisis

Pantalla:

```text
http://localhost:9001
```

Ruta:

```text
datalake/gold/iot/sensor_aula_01/
```

Que ensenar:

```text
hourly_metrics
events_by_hour
anomalies_by_type
battery_hourly
daily_quality
```

Que explicar:

> Gold contiene datasets agregados. Ya no son eventos individuales, sino tablas preparadas para preguntas de negocio: medias por hora, eventos por hora, anomalias, bateria y calidad diaria.

Frase corta:

> Gold responde a la pregunta: que datos preparados consulta el dashboard?

### 10.10 Superset: visualizacion final

Pantalla:

```text
http://localhost:8089
```

Que ensenar:

- grafica de temperatura y humedad por hora;
- eventos por hora;
- anomalias por tipo;
- calidad diaria;
- bateria por hora si la tienes creada.

Que explicar:

> Superset no consulta el CSV raw ni Bronze. Consulta Gold, que ya esta limpio y agregado. Asi el dashboard se centra en visualizar, no en corregir datos.

Frase corta:

> Esta es la parte final del recorrido: el dato ya ha pasado por calidad y se convierte en informacion visual.

### 10.11 Cierre de la demo

Vuelve al flujo mental inicial y resume:

> Hadoop guarda Bronze y cuarentena, Spark valida y transforma, MinIO almacena Silver y Gold, Airflow orquesta y Superset visualiza. Lo importante es que el pipeline no solo mueve datos: controla su calidad y deja evidencias en cada fase.

## 11. Preguntas probables y respuestas

### Cual es el flujo completo?

> ESP32 o simulador genera eventos. Python los prepara como CSV raw. El CSV se guarda en HDFS Bronze. Spark valida reglas de calidad, manda invalidos a cuarentena y validos a Silver en MinIO. Desde Silver se generan agregados Gold en MinIO. Airflow orquesta todo y Superset visualiza Gold.

### Por que arquitectura Medallion?

> Porque separa el dato por niveles de confianza. Bronze conserva el dato original, Silver contiene dato limpio y tipado, y Gold contiene datos agregados listos para negocio.

### Por que HDFS para Bronze?

> Porque HDFS encaja con almacenamiento distribuido y tolerante a fallos para datos brutos. Ademas permite conservar el origen para reprocesar si cambian las reglas.

### Por que MinIO para Silver y Gold?

> Porque MinIO simula un data lake S3. Es comodo para almacenar Parquet y para que herramientas analiticas lean objetos directamente.

### Por que Parquet y no CSV?

> CSV es simple para la ingesta raw, pero Parquet es mejor para analitica: guarda tipos, comprime mejor y permite leer columnas concretas de forma eficiente.

### Que hace Spark?

> Spark lee Bronze, aplica reglas de calidad, convierte tipos, filtra registros validos, crea particiones temporales y genera agregaciones Gold.

### Que reglas de calidad se aplican?

> Columnas obligatorias, timestamp valido, temperatura entre -20 y 80, humedad entre 0 y 100, bateria entre 0 y 100, estado en `OK`, `WARN` o `ERROR`, y deteccion de duplicados por `event_id`.

### Que pasa con los registros malos?

> No entran en Silver. Se escriben en cuarentena HDFS con el campo `error_reason` para saber por que fallaron.

### Que diferencia hay entre validate_raw y quality_bronze?

> `validate_raw` comprueba que el CSV existe, tiene cabecera y columnas obligatorias. `quality_bronze` ya aplica reglas de contenido sobre los datos: tipos, rangos, timestamp, estado y duplicados.

### Que aporta Airflow si ya se puede ejecutar manualmente?

> Airflow convierte la ejecucion en un proceso orquestado: ordena dependencias, guarda logs, permite reintentos y muestra el estado de cada fase.

### Superset lee directamente de MinIO?

> Si. Superset usa DuckDB con la extension `httpfs` para leer ficheros Parquet en `s3://datalake/...` apuntando al endpoint MinIO.

### Que pasa si no hay datos reales del ESP32?

> `collect_iot_batch.py` comprueba cuantos eventos reales hay. Si no llega al minimo, usa un simulador reproducible para que la demo siempre pueda ejecutarse.

### Es tiempo real?

> No es streaming continuo. Es un pipeline batch/orquestado por fecha. El receptor HTTP puede recibir eventos, pero el procesamiento se ejecuta por lotes.

### Que representa `daily_quality`?

> Resume la calidad del dia: total, validos, invalidos, duplicados, valores fuera de rango y porcentaje valido.

### Donde estan las reglas de calidad?

```text
src/quality/rules_iot.yaml
```

### Donde esta el DAG?

```text
src/airflow/dags/iot_medallion_pipeline.py
```

### Donde estan los scripts del pipeline?

```text
src/jobs/
```

## 12. Capturas que conviene tener listas

Minimo:

- Jupyter con el CSV raw.
- HDFS con Bronze.
- HDFS con cuarentena.
- MinIO con Silver.
- MinIO con Gold.
- Airflow con el DAG en verde.
- Superset con el dashboard.
- `quality_bronze_report.json` con los numeros de calidad.

Extra:

- terminal con `Pipeline completed successfully.`;
- SQL de `data/iot/superset/`;
- `docker compose ps` mostrando servicios levantados.

## 13. Comandos utiles durante la defensa

Ver servicios:

```bash
docker compose ps
```

Entrar en Jupyter:

```bash
docker exec -it jupyter-aula bash
```

Ejecutar pipeline completo:

```bash
python src/jobs/run_iot_pipeline.py --date 2026-05-16 --min-real-records 10
```

Reanudar desde una fase:

```bash
python src/jobs/run_iot_pipeline.py --date 2026-05-16 --start-at quality_bronze
```

Listar Bronze en HDFS:

```bash
hdfs dfs -ls /datalake/bronze/iot/sensor_aula_01/year=2026/month=05/day=16
```

Listar cuarentena en HDFS:

```bash
hdfs dfs -ls /datalake/quarantine/iot/sensor_aula_01/year=2026/month=05/day=16
```

Probar receptor HTTP:

```bash
curl http://localhost:5050/health
```

## 14. Frases cortas para sonar seguro

- "Bronze conserva el dato original; Silver contiene dato limpio; Gold contiene datos listos para analisis."
- "La cuarentena evita perder los registros malos y permite auditarlos."
- "Airflow coordina el proceso; no sustituye a Spark ni a Hadoop."
- "Superset visualiza Gold porque el dashboard no deberia limpiar datos."
- "MinIO representa un data lake compatible con S3."
- "El pipeline es batch por fecha, no streaming en tiempo real."
- "Los informes JSON son la trazabilidad de cada ejecucion."

## 15. Riesgos o limitaciones que puedes reconocer

Si te preguntan por mejoras, puedes decir:

- "Ahora trabaja con un unico dispositivo; se podria ampliar a multiples sensores usando `device_id` como particion."
- "La ejecucion es batch; se podria evolucionar a streaming con Kafka o Spark Structured Streaming."
- "YARN esta levantado, pero Spark se ejecuta en local para simplificar la practica."
- "Superset usa SQL sobre Parquet; en un entorno mayor se podria anadir un catalogo tipo Hive Metastore o Iceberg."

## 16. Cierre final

Resumen final:

> Lo importante del proyecto es que no solo mueve datos, sino que controla su calidad. Cada herramienta tiene un papel claro: Hadoop guarda Bronze y cuarentena, Spark valida y transforma, MinIO almacena Silver y Gold, Airflow orquesta y Superset muestra el resultado analitico.
