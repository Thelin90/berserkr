# Applications

Here all the `pyspark` applications are stored.

These files can be scheduled in parallel 

## raw_to_parquet

This application performs an ETL to transform the initial `CSV` data to parquet, which then can be accessed from S3 from any other application.

The though here is that this could run `daily`, `hourly` depending on how often the data updates in the `onlineretailraw` bucket updates.

This will ideally be scheduled by `Airflow`.

### Repartition

Currently the read process is forced to be distributed, the write process is done with native spark `write`.

To improve performance it is important to consider this:

- `Number of Processors:	N`
- `Total Number of Cores:	N`

Retreive on MAC example:

```bash
sysctl hw.physicalcpu hw.logicalcpu
hw.physicalcpu: 2
hw.logicalcpu:  4
```

Retreive info from `spark` example:

```python
print(sc.master)
print(sc.defaultParallelism)
```

- `local[*]`
- `4`

Performance can be investigated at in the spark ui, check the logs when the app starts up:

```bash
19/09/01 12:37:54 INFO SparkUI: Bound SparkUI to 0.0.0.0, and started at http://simons-mbp:4043
```

If you want the `spark-ui` to be alive, so it does not goes down while checking, simply add this in the `src/run_r2p.py`
file:

```python
import time

# load data to S3 in parquet format
rtp.load_online_retail(raw_df)

time.sleep(60)  # seconds to stay alive before UI goes down

# stop spark session
default_spark_session.stop()
```

Note: [article](https://medium.com/@mrpowers/managing-spark-partitions-with-coalesce-and-repartition-4050c57ad5c4) about the subject


