# Applications

Here all the `pyspark` applications are stored.

These files can be scheduled in parallel 

## raw_to_parquet

This application performs an ETL to transform the initial `CSV` data to parquet, which then can be accessed from S3 from any other application.

The though here is that this could run `daily`, `hourly` depending on how often the data updates in the `onlineretailraw` bucket updates.

This will ideally be scheduled by `Airflow`.
