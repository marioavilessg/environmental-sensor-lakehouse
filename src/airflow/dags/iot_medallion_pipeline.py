from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago


default_args = {
    "owner": "mario",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": 30,
}
# Poniendo ds coge la fecha actual
PIPELINE_RUN_DATE = "{{ dag_run.conf.get('run_date', '2026-05-16') }}"


def jupyter_cmd(command: str) -> str:
    return (
        "docker exec jupyter-aula bash -lc "
        "\"cd /home/jovyan/work "
        "&& export PROJECT_ROOT=/home/jovyan/work "
        "&& export PYTHONPATH=/usr/local/spark/python:/usr/local/spark/python/lib/py4j-0.10.9.7-src.zip:/usr/local/spark/python/lib/pyspark.zip:$PYTHONPATH "
        f"&& {command}\""
    )


with DAG(
    dag_id="iot_medallion_pipeline",
    default_args=default_args,
    description="Pipeline IoT ESP32: Bronze HDFS, Silver/Gold MinIO y Superset.",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=["iot", "esp32", "medallon", "hdfs", "minio", "superset"],
) as dag:
    check_or_collect_esp32_data = BashOperator(
        task_id="check_or_collect_esp32_data",
        bash_command=jupyter_cmd(
            f"python src/jobs/ingestion/collect_iot_batch.py --date {PIPELINE_RUN_DATE} --min-real-records 10"
        ),
    )

    validate_raw = BashOperator(
        task_id="validate_raw",
        bash_command=jupyter_cmd(f"python src/jobs/quality/validate_raw.py --date {PIPELINE_RUN_DATE}"),
    )

    load_bronze_hdfs = BashOperator(
        task_id="load_bronze_hdfs",
        bash_command=jupyter_cmd(f"python src/jobs/medallion/load_bronze_hdfs.py --date {PIPELINE_RUN_DATE}"),
    )

    quality_bronze = BashOperator(
        task_id="quality_bronze",
        bash_command=jupyter_cmd(f"python src/jobs/quality/quality_bronze.py --date {PIPELINE_RUN_DATE}"),
    )

    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command=jupyter_cmd(f"python src/jobs/medallion/bronze_to_silver.py --date {PIPELINE_RUN_DATE}"),
    )

    validate_silver = BashOperator(
        task_id="validate_silver",
        bash_command=jupyter_cmd(f"python src/jobs/quality/validate_silver.py --date {PIPELINE_RUN_DATE}"),
    )

    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command=jupyter_cmd(f"python src/jobs/medallion/silver_to_gold.py --date {PIPELINE_RUN_DATE}"),
    )

    publish_gold_superset = BashOperator(
        task_id="publish_gold_superset",
        bash_command=jupyter_cmd(f"python src/jobs/superset/publish_gold_superset.py --date {PIPELINE_RUN_DATE}"),
    )

    (
        check_or_collect_esp32_data
        >> validate_raw
        >> load_bronze_hdfs
        >> quality_bronze
        >> bronze_to_silver
        >> validate_silver
        >> silver_to_gold
        >> publish_gold_superset
    )
