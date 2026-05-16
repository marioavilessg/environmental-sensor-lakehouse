SELECT anomaly_type, SUM(num_anomalies) AS total_anomalies
        FROM read_parquet('s3://datalake/gold/iot/sensor_aula_01/anomalies_by_type/**/*.parquet')
        GROUP BY anomaly_type
        ORDER BY total_anomalies DESC;
