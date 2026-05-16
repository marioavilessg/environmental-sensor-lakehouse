import argparse
import csv
import json
import os
import shutil
import subprocess
import sys
from datetime import date, datetime, timezone
from pathlib import Path


DEVICE_ID = "sensor_aula_01"
BUCKET = "datalake"
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "adminadmin")

REQUIRED_COLUMNS = [
    "event_id",
    "device_id",
    "timestamp",
    "temperature",
    "humidity",
    "battery",
    "status",
    "source",
]

DEFAULT_RULES = {
    "temperature": {"min": -20.0, "max": 80.0},
    "humidity": {"min": 0.0, "max": 100.0},
    "battery": {"min": 0.0, "max": 100.0},
    "status_allowed": ["OK", "WARN", "ERROR"],
    "required_columns": REQUIRED_COLUMNS,
}


def configure_spark_python_path() -> None:
    spark_home = Path(os.getenv("SPARK_HOME", "/usr/local/spark"))
    candidates = [
        spark_home / "python",
        spark_home / "python" / "lib" / "py4j-0.10.9.7-src.zip",
        spark_home / "python" / "lib" / "pyspark.zip",
    ]
    for candidate in candidates:
        if candidate.exists():
            text = str(candidate)
            if text not in sys.path:
                sys.path.insert(0, text)


configure_spark_python_path()


def project_root() -> Path:
    explicit = os.getenv("PROJECT_ROOT")
    if explicit:
        return Path(explicit)
    here = Path(__file__).resolve()
    for parent in here.parents:
        if (parent / "docker-compose.yml").exists():
            return parent
    return here.parents[2]


ROOT = project_root()


def parse_args(description: str, include_min_records: bool = False) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--date", default=os.getenv("PIPELINE_DATE") or date.today().isoformat())
    parser.add_argument("--device-id", default=os.getenv("DEVICE_ID", DEVICE_ID))
    if include_min_records:
        parser.add_argument("--min-real-records", type=int, default=10)
    return parser.parse_args()


def date_parts(run_date: str) -> dict:
    dt = datetime.fromisoformat(run_date).date()
    return {
        "year": f"{dt.year:04d}",
        "month": f"{dt.month:02d}",
        "day": f"{dt.day:02d}",
    }


def inbox_file(run_date: str) -> Path:
    return ROOT / "data" / "iot" / "inbox" / f"esp32_events_{run_date}.csv"


def raw_dir(run_date: str) -> Path:
    return ROOT / "data" / "iot" / "raw" / f"run_date={run_date}"


def raw_batch_file(run_date: str, device_id: str = DEVICE_ID) -> Path:
    return raw_dir(run_date) / f"iot_{device_id}_{run_date}.csv"


def report_dir(run_date: str) -> Path:
    return ROOT / "data" / "iot" / "reports" / f"run_date={run_date}"


def stage_dir(run_date: str, name: str) -> Path:
    return ROOT / "data" / "iot" / "stage" / f"run_date={run_date}" / name


def superset_dir() -> Path:
    return ROOT / "data" / "iot" / "superset"


def hdfs_bronze_path(run_date: str, device_id: str = DEVICE_ID) -> str:
    p = date_parts(run_date)
    return (
        f"/datalake/bronze/iot/{device_id}/"
        f"year={p['year']}/month={p['month']}/day={p['day']}"
    )


def hdfs_quarantine_path(run_date: str, device_id: str = DEVICE_ID) -> str:
    p = date_parts(run_date)
    return (
        f"/datalake/quarantine/iot/{device_id}/"
        f"year={p['year']}/month={p['month']}/day={p['day']}"
    )


def s3_prefix(layer: str, dataset: str, run_date: str | None = None, device_id: str = DEVICE_ID) -> str:
    base = f"{layer}/iot/{device_id}/{dataset}".strip("/")
    if run_date:
        p = date_parts(run_date)
        return f"{base}/year={p['year']}/month={p['month']}/day={p['day']}"
    return base


def s3_uri(layer: str, dataset: str, run_date: str | None = None, device_id: str = DEVICE_ID) -> str:
    return f"s3a://{BUCKET}/{s3_prefix(layer, dataset, run_date, device_id)}"


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def write_json(path: Path, payload: dict) -> None:
    ensure_parent(path)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def read_csv_rows(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as fh:
        return list(csv.DictReader(fh))


def write_csv_rows(path: Path, rows: list[dict], columns: list[str] | None = None) -> None:
    ensure_parent(path)
    columns = columns or REQUIRED_COLUMNS
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def now_utc() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def load_rules() -> dict:
    rules_path = ROOT / "src" / "quality" / "rules_iot.yaml"
    if not rules_path.exists():
        return DEFAULT_RULES
    try:
        import yaml

        loaded = yaml.safe_load(rules_path.read_text(encoding="utf-8")) or {}
        return {**DEFAULT_RULES, **loaded}
    except Exception:
        return DEFAULT_RULES


def run_cmd(command: list[str]) -> None:
    print("+ " + " ".join(command))
    subprocess.run(command, check=True)


def boto3_client():
    import boto3

    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )


def ensure_bucket() -> None:
    client = boto3_client()
    buckets = client.list_buckets().get("Buckets", [])
    if not any(bucket["Name"] == BUCKET for bucket in buckets):
        client.create_bucket(Bucket=BUCKET)


def clear_s3_prefix(prefix: str) -> None:
    client = boto3_client()
    paginator = client.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix.rstrip("/") + "/"):
        keys.extend({"Key": item["Key"]} for item in page.get("Contents", []))
    for item in keys:
        client.delete_object(Bucket=BUCKET, Key=item["Key"])


def upload_dir_to_s3(local_dir: Path, prefix: str) -> None:
    ensure_bucket()
    clear_s3_prefix(prefix)
    client = boto3_client()
    for path in local_dir.rglob("*"):
        if path.is_file():
            if path.name.startswith(".") or path.name == "_SUCCESS":
                continue
            rel = path.relative_to(local_dir).as_posix()
            client.upload_file(str(path), BUCKET, f"{prefix.rstrip('/')}/{rel}")


def download_s3_prefix(prefix: str, local_dir: Path) -> int:
    client = boto3_client()
    if local_dir.exists():
        shutil.rmtree(local_dir)
    local_dir.mkdir(parents=True, exist_ok=True)
    count = 0
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix.rstrip("/") + "/"):
        for item in page.get("Contents", []):
            key = item["Key"]
            rel = key.removeprefix(prefix.rstrip("/") + "/")
            target = local_dir / rel
            target.parent.mkdir(parents=True, exist_ok=True)
            client.download_file(BUCKET, key, str(target))
            count += 1
    return count


def reset_dir(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def local_uri(path: Path) -> str:
    return "file://" + str(path.resolve()).replace("\\", "/")
