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

## PySpark

Apache Spark is the chosen tool used within this project. Spark is quick and very responsive tool to perform data processing with. It provides an analytic engine for large-scale data processing. It is a general distributed in-memory computing framework implemented in scala. Hence spark operates on distributed data collections. However it does not do distributed storage. Spark is proven to be much faster than the popular Apache Hadoop framework. Apache Spark and Hadoop have different purposes but works good together. A high level overview of Apache Spark below:

![Screenshot](/docs/img/spark.png)

In this project the work is done in batches.

### Apache Arrow

In this project `Apache Arrow` will be used and not `Parquet`, `JSON`.

Why? Well simply, when utilising let's say `Parquet`. Which is a great dataformat, but it requires you to use a lot of `CPU` in 
`serialization` and `deserialization` since `PySpark` is being used.

So to avoid that, let's use something that can be utilized in the same memory, here is where `arrow` comes in the picture.

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

## MinIO

Utilizing `MinIO` to be able to run integration test towards `S3`.

## docker-compose

Building everything together with `docker-compose`.

## Run application

Run manually

```bash
some command
```

## Tests

Section to describe how to test the project.

### Unit

```bash
some command
```

### Integration

```bash
some command
```