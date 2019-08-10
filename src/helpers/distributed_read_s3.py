from botocore.client import Config

from typing import List
from pyspark import SparkContext
import logging
import boto3
import os


def distributed_fetch(
    filepath: str,
    s3_bucket,
    endpoint_url,
    signature_version,
    aws_access_key_id,
    aws_secret_access_key,
) -> object:
    """Function fetches file from s3
    :rtype: object
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

    s3.Bucket(s3_bucket).download_file(
        filepath,
        base_path,
    )

    # TODO: another PR to erase downloaded file via subprocess
    #


def get_bucket_files(
    s3_bucket,
    endpoint_url,
    aws_access_key_id,
    aws_secret_access_key,
    signature_version,
):
    files = []
    s3 = boto3.resource(
        's3',
        endpoint_url=endpoint_url,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        config=Config(signature_version=signature_version))

    # Bucket to use
    bucket = s3.Bucket(s3_bucket)

    for file in bucket.objects.all():
        files.append(f'{file.key}')

    return files


class DistributedS3Reader(object):

    def __init__(self, spark_context: SparkContext):
        self.spark_context = spark_context

    def distributed_read_from_s3(
        self,
        s3_bucket,
        endpoint_url,
        aws_access_key_id,
        aws_secret_access_key,
        signature_version,
    ):
        """Function fetches s3 files in a distributed fashion, since S3 does not act as HDFS textFile can't
        be trusted.
        :return: pyspark dataframe
        """
        try:

            files: List = get_bucket_files(
                s3_bucket=s3_bucket,
                endpoint_url=endpoint_url,
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                signature_version=signature_version
            )

            # Force the reading of files to be distributed among the
            # executors. It will basically take the file, split it up into
            # subparts and speed up the read process.
            #

            # Article about the subject: https://tech.kinja.com/how-not-to-pull-from-s3-using-apache-spark-1704509219
            file = self.spark_context.parallelize(files).flatMap(
                lambda filepath: distributed_fetch(
                    filepath=filepath,
                    s3_bucket=s3_bucket,
                    endpoint_url=endpoint_url,
                    aws_access_key_id=aws_access_key_id,
                    aws_secret_access_key=aws_secret_access_key,
                    signature_version=signature_version
                )
            )

            print(file.collect())

        except ValueError:
            logging.warning('download failed')
