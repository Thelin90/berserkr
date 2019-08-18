from src.spark_session import InitSpark
from src.helpers.aws.distributed_read_s3 import DistributedS3Reader
from src.modules.schemas import OnlineRetailSchema

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
        s3_bucket='rawdata',
        endpoint_url='http://127.0.0.1:9000',
        aws_access_key_id='AKIAIOSFODNN7EXAMPLE',
        aws_secret_access_key='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
        signature_version='s3v4',
    )

    # TODO: Move to application that will be called raw_to_parquet
    df = default_spark_session.createDataFrame(raw_rdd, schema=OnlineRetailSchema.INITIAL_SCHEMA)

    print(df.show(5))


main()
