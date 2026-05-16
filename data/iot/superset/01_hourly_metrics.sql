SELECT *
        FROM read_parquet('s3://datalake/gold/iot/sensor_aula_01/hourly_metrics/**/*.parquet')
        ORDER BY date_hour;
