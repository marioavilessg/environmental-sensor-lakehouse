import json
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from common.iot_common import (
    DEVICE_ID,
    hdfs_bronze_path,
    load_rules,
    parse_args,
    report_dir,
    reset_dir,
    local_uri,
    s3_prefix,
    s3_uri,
    stage_dir,
    upload_dir_to_s3,
    write_json,
)
from quality.quality_bronze import build_validation


def main() -> None:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType, StructField, StructType

    args = parse_args("Transforma Bronze HDFS a Silver Parquet en MinIO.")
    rules = load_rules()
    schema = StructType([StructField(col, StringType(), True) for col in rules["required_columns"]])
    spark = SparkSession.builder.appName("iot_bronze_to_silver").master("local[*]").getOrCreate()

    bronze = f"hdfs://namenode:9000{hdfs_bronze_path(args.date, args.device_id)}/*.csv"
    df = spark.read.option("header", "true").schema(schema).csv(bronze)
    checked = build_validation(df, rules)
    valid = (
        checked.filter("is_valid")
        .select(
            "event_id",
            "device_id",
            F.col("event_ts").alias("event_ts"),
            F.col("temperature_d").alias("temperature"),
            F.col("humidity_d").alias("humidity"),
            F.col("battery_d").alias("battery"),
            "status",
            "source",
        )
        .withColumn("year", F.date_format("event_ts", "yyyy"))
        .withColumn("month", F.date_format("event_ts", "MM"))
        .withColumn("day", F.date_format("event_ts", "dd"))
        .withColumn("hour", F.date_format("event_ts", "HH"))
    )

    local_out = stage_dir(args.date, "silver_events")
    reset_dir(local_out)
    valid.coalesce(1).write.mode("overwrite").partitionBy("year", "month", "day").parquet(local_uri(local_out))

    prefix = s3_prefix("silver", "events", None, args.device_id or DEVICE_ID)
    upload_dir_to_s3(local_out, prefix)

    report = {
        "run_date": args.date,
        "bronze_path": bronze,
        "silver_uri": s3_uri("silver", "events", None, args.device_id),
        "valid_records_written": valid.count(),
        "invalid_records_skipped": checked.filter("not is_valid").count(),
    }
    out = report_dir(args.date) / "bronze_to_silver_report.json"
    write_json(out, report)
    print(json.dumps(report, indent=2))
    spark.stop()

    if report["valid_records_written"] == 0:
        raise SystemExit("Silver generation failed: no valid records.")


if __name__ == "__main__":
    main()
