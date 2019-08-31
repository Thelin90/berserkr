from environs import Env
from src.spark_session import InitSpark
from src.helpers.aws.distributed_read_s3 import DistributedS3Reader
from src.modules.schemas import OnlineRetailSchema

env = Env()
env.read_env()


def main():

    # Initialise SparkSession
    init_spark_session = InitSpark("berserkr", "spark-warehouse")
    default_spark_session = init_spark_session.spark_init()
    default_spark_session.sparkContext.setLogLevel("WARN")
    # Enable Arrow-based columnar data transfers
    default_spark_session.conf.set("spark.sql.execution.arrow.enabled", "true")

    dist_s3_reader = DistributedS3Reader(
        spark_context=default_spark_session.sparkContext
    )

    # TODO: read from env file another PR
    raw_rdd = dist_s3_reader.distributed_read_from_s3(
        s3_bucket=env('S3_BUCKET'),
        endpoint_url=env('ENDPOINT_URL'),
        aws_access_key_id=env('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=env('AWS_SECRET_ACCESS_KEY'),
        signature_version=env('SIGNATURE_VERSION'),
    )

    # TODO: Move to application that will be called raw_to_parquet
    df = default_spark_session.createDataFrame(raw_rdd, schema=OnlineRetailSchema.INITIAL_SCHEMA)

    print(df.show(5))


main()
