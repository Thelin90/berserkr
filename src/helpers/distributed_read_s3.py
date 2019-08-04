from botocore.client import Config

from pyspark.sql import SparkSession, Row
import logging
import boto3
import os


def distributed_fetch(
    filepath: str,
    s3_bucket: str,
    endpoint_url: str,
    aws_access_key_id: str,
    aws_secret_access_key: str,
    signature_version: str
) -> object:
    """Function fetches file from s3
    :param filepath: the s3 file path
    :param s3_bucket: the s3 bucket
    :param endpoint_url: specified endpoint
    :param aws_access_key_id: access key for AWS account
    :param aws_secret_access_key: secret key for AWS account
    :param signature_version: AWS signature version
    """

    base_path = os.path.basename(filepath)

    s3 = boto3.resource(
        's3',
        endpoint_url=endpoint_url,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        config=Config(signature_version=signature_version))

    split_filepath = filepath.split('{}/'.format(s3_bucket))

    # Handle both local (Minio) and S3
    # filepaths. When downloading files
    # locally to smoke test, it does not
    # contain 's3://'.
    if len(split_filepath) > 1:
        split_filepath = split_filepath[1]
    else:
        split_filepath = split_filepath[0]

    s3.Bucket(s3_bucket).download_file(
        split_filepath,
        base_path,
    )


class DistributedS3Reader(object):

    def __init__(
        self,
        spark_session: SparkSession,
        spark_context: SparkSession.sparkContext,
        s3_bucket: str,
        endpoint_url: str,
        aws_access_key_id: str,
        aws_secret_access_key: str,
        signature_version: str,
    ):
        self.spark_context = spark_context
        self.spark_session = spark_session
        self.s3_bucket = s3_bucket
        self.endpoint_url = endpoint_url
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.signature_version = signature_version

    def get_bucket_files(self):
        files = []

        s3 = boto3.resource(
            's3',
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            config=Config(signature_version=self.signature_version)
        )

        ## Bucket to use
        bucket = s3.Bucket(self.s3_bucket)

        for file in bucket.objects.all():
            files.append(file.key)

        return files

    def distributed_read_from_s3(self, files):
        """Function fetches s3 files in a distributed fashion, since S3 does not act as HDFS textFile can't
        be trusted.

        :return: pyspark dataframe
        """
        try:
            # Re-assign variables to avoid SparkContext
            # being referenced on self which causes a
            # SPARK-5063 error
            s3_bucket = self.s3_bucket
            endpoint_url = self.endpoint_url
            signature_version = self.signature_version
            aws_access_key_id = self.aws_access_key_id
            aws_secret_access_key = self.aws_secret_access_key

            # Here we force the reading of files to be distributed amongs the
            # executors. It will basically take the file, split it up into
            # subparts and speed up the read process.
            # Article about the subject: https://tech.kinja.com/how-not-to-pull-from-s3-using-apache-spark-1704509219
            self.spark_context.parallelize(files).flatMap(
                lambda filepath: distributed_fetch(
                    filepath,
                    s3_bucket,
                    endpoint_url,
                    aws_access_key_id,
                    aws_secret_access_key,
                    signature_version
                )
            )

        except ValueError:
            logging.warning('download failed')

        return self.spark_session.read.option("delimiter", ',') \
            .option('header', 'true') \
            .csv('onlineretail.csv') \
            .toDF('InvoiceNo', 'StockCode', 'Description', 'Quantity', 'InvoiceDate', 'UnitPrice', 'CustomerID', 'Country')
