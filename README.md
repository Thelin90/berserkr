# berserkr

![Screenshot](/docs/img/berserkr.jpeg)

In old `norse` berserkr is when a viking warrior entered battle wearing nothing for armor but an animal skin.

In this project, `berserkr` will tackle and perform massive machine learning at scale without fear

The goal is to find the likelyhood of a purchase for a certain item for a certain customer.

With the best fitting technologies to solve it.

## Project design

![Screenshot](/docs/img/project-design.png)

## Data processing

In this project the work is done in batches.

### Data format

- files are compressed as `lzo` on the bucket side
- the raw data is then initially cleaned and saved as a parquet.lzo file
- that raw data is then used by the machine learning parts of the application wich will utilize `Apache Arrow`.

#### LZO

Aimed at being very fast, lzop produces files slightly larger than gzip while only requiring a tenth of the CPU use and 
only slightly higher memory utilization, lzop is one of the fastest compressors available.

## PySpark

Apache Spark is the chosen tool used within this project. Spark is quick and very responsive tool to perform data processing with. It provides an analytic engine for large-scale data processing. It is a general distributed in-memory computing framework implemented in scala. Hence spark operates on distributed data collections. However it does not do distributed storage. Spark is proven to be much faster than the popular Apache Hadoop framework. Apache Spark and Hadoop have different purposes but works good together. A high level overview of Apache Spark below:

![Screenshot](/docs/img/spark.png)

In this project the work is done in batches.

### Apache Parquet

Extracted and transformed files will be saved with the `parquet` format in `S3`. And fetched to be used in the 
machine learning parts of the application.

Parquet is a part of the `spark` eco-system and therefore been chosen as the format to store files in `S3` with once
they are prepared and ready.

### Delta Lake

This project will try to utilise `databricks` open source project [Delta Lake](https://delta.io/)

- All data in Delta Lake is stored in Apache Parquet format 
enabling Delta Lake to leverage the efficient compression and encoding schemes that are native to Parquet.

### Apache Arrow

In this project `Apache Arrow` will be utilised when performing operation with `pandas udf` in `spark`.

This will enable better performance in regards to the `CPU` not having to perform `serialization` and `deserialization`.

Some benefits:

* All systems utilize the same memory format
* No overhead for cross-system communication
* Projects can share functionality (eg: Parquet-to-Arrow reader)

A great [video](https://www.youtube.com/watch?v=dPb2ZXnt2_U) explaining this!

### Featuretools

### Pandas UDF

Since PySpark `2.3.0` pandas UDFs are now available. This opens up amazing opportunities to work with the data with help of pandas to perform machine learning at massive scale.

Here is when `Apache Arrow` dataformat comes in and play a crucial part. Since we utilize `Pandas UDF`, we will be able to loose the overhead of going from a dataformat in the `Spark JVM` to `Python`. Basically we will have the same dataformat comming in and out from the calculations.

`amazing right? =D`

### Dataset

Dataset has been fetched from 

* https://archive.ics.uci.edu/ml/datasets/Online%20Retail

## Airflow

Using `Apache Airflow` to schedule the applications within the project.

I a normal scenario this should be in its own repository but will keep on here just because I want to share this project as an open source example.

## Run application

Run manually by first initialise `virtualenv`  

```bash
virtualenv venv -p python3.7
source venv/bin/activate
```

Then install dependency packages:

Go to:
```bash
cd scripts
```
Run:
```bash
./run.sh
```

Explicitly for MacOS users

```bash
brew install lzop
```

`subprocess` will otherwise fail when running application locally (decompression when reading S3 files).

#### Local run

`cd tools/docker`

```bash
docker-compose up
```

A [minio](http://127.0.0.1:9000/minio/rawdata/) server with a default bucket and data has now been created, which can be used.


### Online retailer app
Run:
```bash
make onlineretailer
```

[Custom S3 endpoints with Spark](https://gist.github.com/tobilg/e03dbc474ba976b9f235)
[LZO compression spark]((https://github.com/twitter/hadoop-lzo))

## Tests

Section to describe how to test the project.

### Unit

```bash
make test
```

### Integration

```bash
some command
```