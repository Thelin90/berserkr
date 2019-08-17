import boto3
import os

from src.helpers.subprocesses.decompress import decompress
from botocore.client import Config
from typing import Iterator


def distributed_fetch(
    filepath: str,
    s3_bucket: str,
    endpoint_url: str,
    aws_access_key_id: str,
    aws_secret_access_key: str,
    signature_version: str,
) -> Iterator:
    """Function fetches file from s3/Minio bucket
    :rtype: object
    :param filepath: the s3/minio file path
    :param s3_bucket: the s3/minio bucket
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

    # TODO: another PR to decompress and erase downloaded file via subprocess
    return decompress(base_path)


def get_bucket_files(
    endpoint_url: str,
    s3_bucket: str,
    aws_access_key_id: str,
    aws_secret_access_key: str,
    signature_version: str,
):
    """Function fetches the available files on a given s3/minio bucket

    :param endpoint_url: specified endpoint
    :param s3_bucket: the s3/minio bucket
    :param aws_access_key_id: access key for AWS account
    :param aws_secret_access_key: secret key for AWS account
    :param signature_version: AWS signature version
    :return: list of available files
    """
    files = []

    # TODO: could perhaps be in the distributed_read_from_s3? Not running on several executors
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
