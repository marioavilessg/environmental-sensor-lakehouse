import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from common.iot_common import DEVICE_ID, parse_args, s3_prefix, superset_dir


ENGINE_PARAMETERS = """{
  "connect_args": {
    "preload_extensions": ["httpfs"],
    "config": {
      "s3_endpoint": "minio:9000",
      "s3_access_key_id": "admin",
      "s3_secret_access_key": "adminadmin",
      "s3_use_ssl": false,
      "s3_url_style": "path"
    }
  }
}"""


def write_sql(path, sql):
    path.write_text(sql.strip() + "\n", encoding="utf-8")


def main() -> None:
    args = parse_args("Prepara SQL para Superset sobre Gold MinIO.")
    device_id = args.device_id or DEVICE_ID
    out = superset_dir()
    out.mkdir(parents=True, exist_ok=True)

    hourly = f"s3://datalake/{s3_prefix('gold', 'hourly_metrics', None, device_id)}/**/*.parquet"
    anomalies = f"s3://datalake/{s3_prefix('gold', 'anomalies_by_type', None, device_id)}/**/*.parquet"
    quality = f"s3://datalake/{s3_prefix('gold', 'daily_quality', None, device_id)}/**/*.parquet"
    battery = f"s3://datalake/{s3_prefix('gold', 'battery_hourly', None, device_id)}/**/*.parquet"
    events = f"s3://datalake/{s3_prefix('gold', 'events_by_hour', None, device_id)}/**/*.parquet"

    write_sql(
        out / "01_hourly_metrics.sql",
        f"""
        SELECT *
        FROM read_parquet('{hourly}')
        ORDER BY date_hour;
        """,
    )
    write_sql(
        out / "02_anomalies_by_type.sql",
        f"""
        SELECT anomaly_type, SUM(num_anomalies) AS total_anomalies
        FROM read_parquet('{anomalies}')
        GROUP BY anomaly_type
        ORDER BY total_anomalies DESC;
        """,
    )
    write_sql(
        out / "03_daily_quality.sql",
        f"""
        SELECT *
        FROM read_parquet('{quality}')
        ORDER BY event_date;
        """,
    )
    write_sql(
        out / "04_battery_hourly.sql",
        f"""
        SELECT *
        FROM read_parquet('{battery}')
        ORDER BY date_hour;
        """,
    )
    write_sql(
        out / "05_events_by_hour.sql",
        f"""
        SELECT *
        FROM read_parquet('{events}')
        ORDER BY date_hour;
        """,
    )
    (out / "duckdb_connection.md").write_text(
        "# Superset DuckDB connection\n\n"
        "URI: `duckdb:////tmp/superset_lakehouse.db`\n\n"
        "Advanced > Other > Engine Parameters:\n\n"
        f"```json\n{ENGINE_PARAMETERS}\n```\n",
        encoding="utf-8",
    )
    print(f"Superset SQL files written to {out}")


if __name__ == "__main__":
    main()
