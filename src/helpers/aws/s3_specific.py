import boto3
import os

from src.helpers.subprocesses.decompress import decompress_lzo
from botocore.client import Config
from typing import List


def distributed_fetch(
    filepath: str,
    s3_bucket: str,
    endpoint_url: str,
    aws_access_key_id: str,
    aws_secret_access_key: str,
    signature_version: str,
) -> List[str]:
    """Function fetches file from s3/Minio bucket
    :rtype: object
    :param filepath: the s3/minio file path
    :param s3_bucket: the s3/minio bucket
    :param endpoint_url: specified endpoint
    :param aws_access_key_id: access key for AWS account
    :param aws_secret_access_key: secret key for AWS account
    :param signature_version: AWS signature version
    """

    base_path: str = os.path.basename(filepath)

    s3: boto3.resource = boto3.resource(
        's3',
        endpoint_url=endpoint_url,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        config=Config(signature_version=signature_version))

    s3.Bucket(s3_bucket).download_file(
        filepath,
        base_path,
    )

    return decompress_lzo(base_path)


def delete_bucket_data(filesystem, uri, s3_url, sc, path):
    """df.write.mode='overwrite' does not seem to work with parquet files O.o?

    https://stackoverflow.com/questions/44991550/aws-emr-spark-error-writing-to-s3-illegalargumentexception-cannot-create-a
    https://www.quora.com/How-do-you-overwrite-the-output-directory-when-using-PySpark
    http://crazyslate.com/how-to-rename-hadoop-files-using-wildcards-while-patterns/

    :param filesystem:
    :param uri:
    :param s3_url:
    :param sc:
    :param path:
    :return:
    """
    fs = filesystem.get(uri(s3_url), sc._jsc.hadoopConfiguration())
    file_status = fs.globStatus(path("/*"))
    for status in file_status:
        fs.delete(status.getPath(), True)


def get_bucket_files(
    endpoint_url: str,
    s3_bucket: str,
    aws_access_key_id: str,
    aws_secret_access_key: str,
    signature_version: str,
) -> List[str]:
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
    s3: boto3.resource = boto3.resource(
        's3',
        endpoint_url=endpoint_url,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        config=Config(signature_version=signature_version))

    # Bucket to use
    bucket: s3.Bucket = s3.Bucket(s3_bucket)

    for file in bucket.objects.all():
        files.append(f'{file.key}')

    return files
