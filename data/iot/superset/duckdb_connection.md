# Superset DuckDB connection

URI: `duckdb:////tmp/superset_lakehouse.db`

Advanced > Other > Engine Parameters:

```json
{
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
}
```
