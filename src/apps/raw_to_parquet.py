from datetime import datetime
from pyspark import RDD, SparkContext, SQLContext
from pyspark.sql import SparkSession, DataFrame

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
            s3_bucket,
            endpoint_url,
            aws_access_key_id,
            aws_secret_access_key,
            signature_version,
            schema
    ):
        self.spark_session: SparkSession = spark_session
        self.s3_bucket: str = s3_bucket
        self.endpoint_url: str = endpoint_url
        self.aws_access_key_id: str = aws_access_key_id
        self.aws_secret_access_key: str = aws_secret_access_key
        self.signature_version: str = signature_version
        self.schema: str = schema
        self.raw_rdd: RDD = self.spark_session.sparkContext.emptyRDD()
        self.df = self.spark_session.createDataFrame(self.raw_rdd, OnlineRetailSchema.EMPTY_SCHEMA)

    def extract(self) -> None:
        """Method to extract dataset distributed, generic

        :return:
        """
        dist_s3_reader: DistributedS3Reader = DistributedS3Reader(
            spark_context=self.spark_session.sparkContext
        )

        self.raw_rdd: RDD = dist_s3_reader.distributed_read_from_s3(
            s3_bucket=self.s3_bucket,
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            signature_version=self.signature_version,
        )

    def transform_online_retail(self) -> None:
        """Method to transform online retail dataset to its correct dataformats, specific
        for online retail

        :return:
        """
        raw_rdd = self.raw_rdd.map(lambda retail: (
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

        self.df: DataFrame = self.spark_session.createDataFrame(
            raw_rdd,
            schema=self.schema
        )

    def load_online_retail(self) -> None:
        """Method to load data as parquet to S3, specific for online retail

        :param df:
        :return:
        """
        df: DataFrame = self.df
        print(df.show(1000))

        s3_url = 's3a://onlineretailparquet/'
        sc = self.spark_session.sparkContext

        uri = sc._gateway.jvm.java.net.URI
        path = sc._gateway.jvm.org.apache.hadoop.fs.Path
        filesystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem

        delete_bucket_data(
            filesystem=filesystem,
            uri=uri,
            path=path,
            sc=sc,
            s3_url=s3_url,
        )

        # write table to S3
        df.write.parquet(s3_url)
