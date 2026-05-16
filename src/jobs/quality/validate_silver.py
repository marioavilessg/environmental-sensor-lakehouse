import json
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from common.iot_common import (
    DEVICE_ID,
    date_parts,
    download_s3_prefix,
    local_uri,
    parse_args,
    report_dir,
    s3_prefix,
    stage_dir,
    write_json,
)


def main() -> None:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    args = parse_args("Valida la capa Silver en MinIO.")
    prefix = s3_prefix("silver", "events", None, args.device_id or DEVICE_ID)
    local = stage_dir(args.date, "silver_download")
    files = download_s3_prefix(prefix, local)
    if files == 0:
        raise FileNotFoundError(f"No objects found in s3://datalake/{prefix}")

    p = date_parts(args.date)
    spark = SparkSession.builder.appName("iot_validate_silver").master("local[*]").getOrCreate()
    df = spark.read.parquet(local_uri(local)).filter(
        (F.col("year").cast("string") == p["year"])
        & (F.format_string("%02d", F.col("month").cast("int")) == p["month"])
        & (F.format_string("%02d", F.col("day").cast("int")) == p["day"])
    )

    total = df.count()
    invalid = df.filter(
        "event_id is null or device_id is null or event_ts is null "
        "or temperature is null or humidity is null or battery is null "
        "or temperature < -20 or temperature > 80 "
        "or humidity < 0 or humidity > 100 "
        "or battery < 0 or battery > 100"
    ).count()

    report = {
        "run_date": args.date,
        "silver_prefix": f"s3://datalake/{prefix}",
        "downloaded_objects": files,
        "records_checked": total,
        "invalid_records": invalid,
    }
    write_json(report_dir(args.date) / "validate_silver_report.json", report)
    print(json.dumps(report, indent=2))
    spark.stop()

    if total == 0 or invalid > 0:
        raise SystemExit("Silver validation failed.")


if __name__ == "__main__":
    main()
