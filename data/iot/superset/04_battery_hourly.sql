SELECT *
        FROM read_parquet('s3://datalake/gold/iot/sensor_aula_01/battery_hourly/**/*.parquet')
        ORDER BY date_hour;
