SELECT *
        FROM read_parquet('s3://datalake/gold/iot/sensor_aula_01/daily_quality/**/*.parquet')
        ORDER BY event_date;
