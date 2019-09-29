from pyspark import RDD
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType

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
            raw_format: str,
            transform_call,
            schema: StructType
    ):
        self.spark_session: SparkSession = spark_session
        self.raw_s3_bucket: str = raw_s3_bucket
        self.parquet_s3_bucket: str = parquet_s3_bucket
        self.aws_endpoint_url: str = aws_endpoint_url
        self.aws_access_key_id: str = aws_access_key_id
        self.aws_secret_access_key: str = aws_secret_access_key
        self.signature_version: str = signature_version
        self.raw_format: str = raw_format
        self.schema: StructType = schema
        self.transform_call = transform_call
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
            raw_format=self.raw_format,
        )

    def transform(self, rdd: RDD) -> DataFrame:
        return self.transform_call(
            sc=self.spark_session,
            raw_rdd=rdd,
            schema=self.schema,
            max_month=MAX_MONTH,
        )

    def load(self, raw_df: DataFrame) -> None:
        """Method to load data as parquet to S3

        Note, if needed, the spark session can delete files in the bucket with this code:

        from src.helpers.aws.s3_specific import delete_bucket_data

        delete_bucket_data(
            sc=self.spark_session.sparkContext,
            s3_url=s3_url,
        )

        Also another note, use when necessary

        number_of_partitions = sc.defaultParallelism * 4
        raw_df = raw_df.repartition(number_of_partitions)

        :param raw_df:
        :return:
        """
        s3_url = f's3a://{self.parquet_s3_bucket}/'

        # raw to parquet dataframe schema
        raw_df.printSchema()

        # write table to S3
        # https://medium.com/@mrpowers/managing-spark-partitions-with-coalesce-and-repartition-4050c57ad5c4
        # TODO: enable LZO compression https://github.com/twitter/hadoop-lzo
        raw_df.coalesce(1).write.format("delta").mode("overwrite").save(s3_url)
