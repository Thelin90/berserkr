from src.apps.spark_session import InitSpark
from pyspark.sql import DataFrame

from src.helpers.distributed_read_s3 import DistributedS3Reader


def main():
    # Initialise SparkSession
    init_spark_session = InitSpark("berserkr", "spark-warehouse")
    default_spark_session = init_spark_session.spark_init()
    default_spark_session.sparkContext.setLogLevel("WARN")
    # Enable Arrow-based columnar data transfers
    default_spark_session.conf.set("spark.sql.execution.arrow.enabled", "true")

    dist_s3_reader = DistributedS3Reader(
        spark_session=default_spark_session,
        spark_context=default_spark_session.sparkContext,
        s3_bucket='rawdata',
        endpoint_url='http://127.0.0.1:9000',
        aws_access_key_id='AKIAIOSFODNN7EXAMPLE',
        aws_secret_access_key='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
        signature_version='s3v4',
    )

    files = dist_s3_reader.get_bucket_files()

    df: DataFrame = dist_s3_reader.distributed_read_from_s3(files)

    df.show(10)


main()
