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
    reset_dir,
    s3_prefix,
    s3_uri,
    stage_dir,
    upload_dir_to_s3,
    write_json,
)


def upload_dataset(df, run_date: str, device_id: str, dataset: str) -> tuple[str, int]:
    local_out = stage_dir(run_date, f"gold_{dataset}")
    reset_dir(local_out)
    df.coalesce(1).write.mode("overwrite").parquet(local_uri(local_out))
    prefix = s3_prefix("gold", dataset, None, device_id)
    upload_dir_to_s3(local_out, prefix)
    return s3_uri("gold", dataset, None, device_id), df.count()


def main() -> None:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    args = parse_args("Construye datasets Gold desde Silver.")
    device_id = args.device_id or DEVICE_ID
    silver_prefix = s3_prefix("silver", "events", None, device_id)
    local_silver = stage_dir(args.date, "silver_for_gold")
    files = download_s3_prefix(silver_prefix, local_silver)
    if files == 0:
        raise FileNotFoundError(f"No Silver objects found in s3://datalake/{silver_prefix}")

    spark = SparkSession.builder.appName("iot_silver_to_gold").master("local[*]").getOrCreate()
    p = date_parts(args.date)
    silver = spark.read.parquet(local_uri(local_silver)).filter(
        (F.col("year").cast("string") == p["year"])
        & (F.format_string("%02d", F.col("month").cast("int")) == p["month"])
        & (F.format_string("%02d", F.col("day").cast("int")) == p["day"])
    )

    enriched = silver.withColumn("date_hour", F.date_format("event_ts", "yyyy-MM-dd HH:00:00")).withColumn(
        "event_date", F.to_date("event_ts")
    )

    hourly_metrics = (
        enriched.groupBy("device_id", "date_hour")
        .agg(
            F.avg("temperature").alias("avg_temperature"),
            F.max("temperature").alias("max_temperature"),
            F.min("temperature").alias("min_temperature"),
            F.avg("humidity").alias("avg_humidity"),
            F.max("humidity").alias("max_humidity"),
            F.min("humidity").alias("min_humidity"),
            F.avg("battery").alias("avg_battery"),
            F.min("battery").alias("min_battery"),
            F.count("*").alias("num_events"),
        )
        .orderBy("date_hour")
    )

    battery_hourly = (
        enriched.groupBy("device_id", "date_hour")
        .agg(F.avg("battery").alias("avg_battery"), F.min("battery").alias("min_battery"))
        .orderBy("date_hour")
    )

    events_by_hour = (
        enriched.groupBy("device_id", "date_hour")
        .agg(F.count("*").alias("num_events"))
        .orderBy("date_hour")
    )

    anomaly_flags = enriched.select(
        "device_id",
        "date_hour",
        F.array(
            F.when(F.col("temperature") >= 30, F.lit("temperature_high")),
            F.when(F.col("humidity") <= 35, F.lit("humidity_low")),
            F.when(F.col("battery") <= 20, F.lit("battery_low")),
            F.when(F.col("status") != "OK", F.concat(F.lit("status_"), F.lower("status"))),
        ).alias("anomalies"),
    )
    anomalies_by_type = (
        anomaly_flags.select("device_id", "date_hour", F.explode("anomalies").alias("anomaly_type"))
        .filter("anomaly_type is not null")
        .groupBy("device_id", "date_hour", "anomaly_type")
        .agg(F.count("*").alias("num_anomalies"))
        .orderBy("date_hour", "anomaly_type")
    )

    quality_report_path = report_dir(args.date) / "quality_bronze_report.json"
    if quality_report_path.exists():
        quality = json.loads(quality_report_path.read_text(encoding="utf-8"))
    else:
        quality = {
            "total_records": enriched.count(),
            "valid_records": enriched.count(),
            "invalid_records": 0,
            "duplicates": 0,
            "out_of_range_values": 0,
        }
    total_records = int(quality.get("total_records", 0))
    valid_records = int(quality.get("valid_records", 0))
    daily_quality = spark.createDataFrame(
        [
            {
                "device_id": device_id,
                "event_date": args.date,
                "total_records": total_records,
                "valid_records": valid_records,
                "invalid_records": int(quality.get("invalid_records", 0)),
                "duplicates": int(quality.get("duplicates", 0)),
                "out_of_range_values": int(quality.get("out_of_range_values", 0)),
                "valid_percentage": round((valid_records / total_records) * 100, 2) if total_records else 0.0,
            }
        ]
    )

    outputs = {}
    for name, dataset in {
        "hourly_metrics": hourly_metrics,
        "battery_hourly": battery_hourly,
        "events_by_hour": events_by_hour,
        "anomalies_by_type": anomalies_by_type,
        "daily_quality": daily_quality,
    }.items():
        uri, count = upload_dataset(dataset, args.date, device_id, name)
        outputs[name] = {"uri": uri, "records": count}

    report = {"run_date": args.date, "silver_prefix": f"s3://datalake/{silver_prefix}", "gold_outputs": outputs}
    write_json(report_dir(args.date) / "silver_to_gold_report.json", report)
    print(json.dumps(report, indent=2))
    spark.stop()


if __name__ == "__main__":
    main()
