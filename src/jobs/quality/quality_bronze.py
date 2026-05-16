import json
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from common.iot_common import hdfs_bronze_path, hdfs_quarantine_path, load_rules, parse_args, report_dir, write_json


def build_validation(df, rules):
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    temp_min = float(rules["temperature"]["min"])
    temp_max = float(rules["temperature"]["max"])
    hum_min = float(rules["humidity"]["min"])
    hum_max = float(rules["humidity"]["max"])
    bat_min = float(rules["battery"]["min"])
    bat_max = float(rules["battery"]["max"])
    allowed = rules["status_allowed"]

    w = Window.partitionBy("event_id")
    checked = (
        df.withColumn("temperature_d", F.col("temperature").cast("double"))
        .withColumn("humidity_d", F.col("humidity").cast("double"))
        .withColumn("battery_d", F.col("battery").cast("double"))
        .withColumn("event_ts", F.to_timestamp("timestamp"))
        .withColumn("event_id_count", F.count("*").over(w))
    )

    reasons = [
        F.when(F.col("event_id").isNull() | (F.col("event_id") == ""), "missing_event_id"),
        F.when(F.col("device_id").isNull() | (F.col("device_id") == ""), "missing_device_id"),
        F.when(F.col("timestamp").isNull() | (F.col("timestamp") == ""), "missing_timestamp"),
        F.when(F.col("event_ts").isNull(), "invalid_timestamp"),
        F.when(F.col("temperature").isNull(), "missing_temperature"),
        F.when(F.col("temperature").isNotNull() & F.col("temperature_d").isNull(), "invalid_temperature_type"),
        F.when((F.col("temperature_d") < temp_min) | (F.col("temperature_d") > temp_max), "temperature_out_of_range"),
        F.when(F.col("humidity").isNull(), "missing_humidity"),
        F.when(F.col("humidity").isNotNull() & F.col("humidity_d").isNull(), "invalid_humidity_type"),
        F.when((F.col("humidity_d") < hum_min) | (F.col("humidity_d") > hum_max), "humidity_out_of_range"),
        F.when(F.col("battery").isNull(), "missing_battery"),
        F.when(F.col("battery").isNotNull() & F.col("battery_d").isNull(), "invalid_battery_type"),
        F.when((F.col("battery_d") < bat_min) | (F.col("battery_d") > bat_max), "battery_out_of_range"),
        F.when(~F.col("status").isin(allowed), "invalid_status"),
        F.when(F.col("event_id").isNotNull() & (F.col("event_id_count") > 1), "duplicate_event_id"),
    ]
    return checked.withColumn("error_reason", F.concat_ws("|", *reasons)).withColumn(
        "is_valid", F.col("error_reason") == ""
    )


def main() -> None:
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StringType, StructField, StructType

    args = parse_args("Ejecuta reglas de calidad sobre Bronze HDFS.")
    rules = load_rules()
    schema = StructType([StructField(col, StringType(), True) for col in rules["required_columns"]])
    spark = SparkSession.builder.appName("iot_quality_bronze").master("local[*]").getOrCreate()

    bronze = f"hdfs://namenode:9000{hdfs_bronze_path(args.date, args.device_id)}/*.csv"
    df = spark.read.option("header", "true").schema(schema).csv(bronze)
    checked = build_validation(df, rules)
    invalid = checked.filter(~checked.is_valid)
    quarantine = f"hdfs://namenode:9000{hdfs_quarantine_path(args.date, args.device_id)}"
    invalid.drop("is_valid").coalesce(1).write.mode("overwrite").json(quarantine)

    total = checked.count()
    invalid_count = invalid.count()
    valid_count = total - invalid_count
    duplicate_count = checked.filter("event_id_count > 1").count()
    null_timestamps = checked.filter("timestamp is null or timestamp = ''").count()
    out_of_range = checked.filter(
        "error_reason like '%temperature_out_of_range%' "
        "or error_reason like '%humidity_out_of_range%' "
        "or error_reason like '%battery_out_of_range%'"
    ).count()

    report = {
        "run_date": args.date,
        "bronze_path": bronze,
        "quarantine_path": quarantine,
        "total_records": total,
        "valid_records": valid_count,
        "invalid_records": invalid_count,
        "duplicates": duplicate_count,
        "null_timestamps": null_timestamps,
        "out_of_range_values": out_of_range,
        "rules": rules,
    }
    out = report_dir(args.date) / "quality_bronze_report.json"
    write_json(out, report)
    print(json.dumps(report, indent=2))
    spark.stop()

    if total == 0:
        raise SystemExit("Bronze quality failed: no records found.")


if __name__ == "__main__":
    main()
