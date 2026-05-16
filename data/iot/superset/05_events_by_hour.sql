SELECT *
        FROM read_parquet('s3://datalake/gold/iot/sensor_aula_01/events_by_hour/**/*.parquet')
        ORDER BY date_hour;
