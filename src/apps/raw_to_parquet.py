from datetime import datetime
from pyspark import RDD
from pyspark.sql import SparkSession, DataFrame

from src.helpers.aws.distributed_read_s3 import DistributedS3Reader

MAX_MONTH = 12


class RawToParquet(object):
    """

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

    def transform_online_retail(self) -> DataFrame:
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

        return self.spark_session.createDataFrame(
            raw_rdd,
            schema=self.schema
        )

    @staticmethod
    def load_online_retail(df: DataFrame) -> None:
        """Method to load data as parquet to S3, specific for online retail

        :param df:
        :return:
        """
        print(df.show(1000))

        # convert dataframe to parquet
        df.write.format('parquet').mode('overwrite').saveAsTable('test_table')

        # write table to S3

        # delete parquet file form machine
