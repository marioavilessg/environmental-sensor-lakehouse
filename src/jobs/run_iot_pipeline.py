import argparse
import os
import subprocess
import sys
from datetime import date
from pathlib import Path


ROOT = Path(os.getenv("PROJECT_ROOT", Path(__file__).resolve().parents[2]))
SPARK_PYTHONPATH = (
    "/usr/local/spark/python:"
    "/usr/local/spark/python/lib/py4j-0.10.9.7-src.zip:"
    "/usr/local/spark/python/lib/pyspark.zip"
)

STEPS = [
    ("collect_iot_batch", "src/jobs/ingestion/collect_iot_batch.py"),
    ("validate_raw", "src/jobs/quality/validate_raw.py"),
    ("load_bronze_hdfs", "src/jobs/medallion/load_bronze_hdfs.py"),
    ("quality_bronze", "src/jobs/quality/quality_bronze.py"),
    ("bronze_to_silver", "src/jobs/medallion/bronze_to_silver.py"),
    ("validate_silver", "src/jobs/quality/validate_silver.py"),
    ("silver_to_gold", "src/jobs/medallion/silver_to_gold.py"),
    ("publish_gold_superset", "src/jobs/superset/publish_gold_superset.py"),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ejecuta el pipeline IoT completo en orden.")
    parser.add_argument("--date", default=os.getenv("PIPELINE_DATE") or date.today().isoformat())
    parser.add_argument("--device-id", default=os.getenv("DEVICE_ID", "sensor_aula_01"))
    parser.add_argument("--min-real-records", type=int, default=10)
    parser.add_argument(
        "--start-at",
        choices=[name for name, _ in STEPS],
        default=STEPS[0][0],
        help="Permite reanudar desde una fase concreta.",
    )
    parser.add_argument(
        "--stop-after",
        choices=[name for name, _ in STEPS],
        default=STEPS[-1][0],
        help="Permite detenerse despues de una fase concreta.",
    )
    return parser.parse_args()


def command_for_step(step: str, script: str, args: argparse.Namespace) -> list[str]:
    command = [sys.executable, script, "--date", args.date, "--device-id", args.device_id]
    if step == "collect_iot_batch":
        command.extend(["--min-real-records", str(args.min_real_records)])
    return command


def main() -> None:
    args = parse_args()
    step_names = [name for name, _ in STEPS]
    start = step_names.index(args.start_at)
    stop = step_names.index(args.stop_after)
    if start > stop:
        raise SystemExit("--start-at no puede ir despues de --stop-after.")

    env = os.environ.copy()
    env["PROJECT_ROOT"] = str(ROOT)
    current_pythonpath = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = f"{SPARK_PYTHONPATH}:{current_pythonpath}" if current_pythonpath else SPARK_PYTHONPATH

    print(f"Running IoT pipeline for date={args.date}, device_id={args.device_id}")
    for step, script in STEPS[start : stop + 1]:
        command = command_for_step(step, script, args)
        print("\n" + "=" * 80)
        print(f"STEP: {step}")
        print("+ " + " ".join(command))
        subprocess.run(command, cwd=ROOT, env=env, check=True)

    print("\nPipeline completed successfully.")


if __name__ == "__main__":
    main()

