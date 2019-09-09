from datetime import datetime

from pyspark import RDD
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType

from src.helpers.aws.s3_specific import delete_bucket_data
from src.modules.schemas import OnlineRetailSchema
from src.helpers.aws.distributed_read_s3 import DistributedS3Reader

MAX_MONTH = 12


class RawToParquet(object):
    """
    This class will clean the data, perhaps create some new features from the existing one if possible,
    then save it on S3 as a parquet file.


    The machine learning app of the project will then read this file, and utilize pandas udf, featuretools
    """
    def __init__(
            self,
            spark_session: SparkSession,
            raw_s3_bucket: str,
            parquet_s3_bucket: str,
            aws_endpoint_url: str,
            aws_access_key_id: str,
            aws_secret_access_key: str,
            signature_version: str,
            schema: StructType
    ):
        self.spark_session: SparkSession = spark_session
        self.raw_s3_bucket: str = raw_s3_bucket
        self.parquet_s3_bucket: str = parquet_s3_bucket
        self.aws_endpoint_url: str = aws_endpoint_url
        self.aws_access_key_id: str = aws_access_key_id
        self.aws_secret_access_key: str = aws_secret_access_key
        self.signature_version: str = signature_version
        self.schema: StructType = schema
        self.raw_rdd: RDD = self.spark_session.sparkContext.emptyRDD()
        self.df: DataFrame = self.spark_session.createDataFrame(self.raw_rdd, OnlineRetailSchema.EMPTY_SCHEMA)

    def extract(self) -> RDD:
        """Method to extract dataset distributed, generic

        :return:
        """
        dist_s3_reader: DistributedS3Reader = DistributedS3Reader(
            spark_context=self.spark_session.sparkContext
        )

        return dist_s3_reader.distributed_read_from_s3(
            s3_bucket=self.raw_s3_bucket,
            endpoint_url=self.aws_endpoint_url,
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            signature_version=self.signature_version,
        )

    def transform_online_retail(self, raw_rdd: RDD) -> DataFrame:
        """Method to transform online retail dataset to its correct dataformats, specific
        for online retail

        :return:
        """

        # initial transformation of the raw RDD
        raw_rdd = raw_rdd.map(lambda retail: (
            retail[0],  # InvoiceNo
            retail[1],  # StockCode
            retail[2] if retail[2] != '' else None,  # Description
            int(retail[3]),  # Quantity
            datetime.strptime(retail[4], '%d/%m/%Y %H:%M') if
            int(retail[4].split('/')[1]) < MAX_MONTH
            else datetime.strptime(retail[4], '%m/%d/%Y %H:%M'),  # InvoiceDate
            float(retail[5]),  # UnitPrice
            int(retail[6]) if retail[6] != '' else None,  # CustomerID
            retail[7] if retail[7] != '' else None)  # Country
        )

        return self.spark_session.createDataFrame(
            raw_rdd,
            schema=self.schema
        )

    def load_online_retail(self, raw_df: DataFrame) -> None:
        """Method to load data as parquet to S3, specific for online retail

        :param raw_df:
        :return:
        """
        s3_url = f's3a://{self.parquet_s3_bucket}/'
        sc = self.spark_session.sparkContext

        # delete data from bucket
        delete_bucket_data(
            sc=sc,
            s3_url=s3_url,
        )

        # raw to parquet dataframe schema
        raw_df.printSchema()

        # use when necessary
        # number_of_partitions = sc.defaultParallelism * 4
        # raw_df = raw_df.repartition(number_of_partitions)

        # write table to S3
        # https://medium.com/@mrpowers/managing-spark-partitions-with-coalesce-and-repartition-4050c57ad5c4
        # TODO: enable LZO compression https://github.com/twitter/hadoop-lzo
        raw_df.coalesce(1).write.format("delta").save(s3_url)
