"""This python scripts run raw to parquet which performs an ETL on the raw data to parquet

- This file can be run with spark-submit individually

"""

from environs import Env

from pyspark import RDD
from pyspark.sql import DataFrame

from src.spark_session import InitSpark
from src.apps.raw_to_parquet import RawToParquet
from src.modules.schemas import OnlineRetailSchema

# initialise Env
env: Env = Env()

# read env variables
env.read_env()

# read env variables
warehouse_location = env('RAW_TO_PARQUET_WAREHOUSE_LOCATION')
app_name = env('RAW_TO_PARQUET_APP_NAME')
raw_s3_bucket = env('RAW_DATA_ONLINE_RETAIL_S3_BUCKET')
parquet_s3_bucket = env('PARQUET_DATA_ONLINE_RETAIL_S3_BUCKET')
aws_endpoint_url = env('AWS_ENDPOINT_URL')
aws_access_key_id = env('AWS_ACCESS_KEY_ID')
aws_secret_access_key = env('AWS_SECRET_ACCESS_KEY')
signature_version = env('SIGNATURE_VERSION')


# Initialise SparkSession
init_spark_session = InitSpark(
    app_name=app_name,
    warehouse_location=warehouse_location,
    aws_endpoint_url=aws_endpoint_url,
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key
)

default_spark_session = init_spark_session.spark_init()

rtp = RawToParquet(
    spark_session=default_spark_session,
    raw_s3_bucket=raw_s3_bucket,
    parquet_s3_bucket=parquet_s3_bucket,
    aws_endpoint_url=aws_endpoint_url,
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    signature_version=signature_version,
    schema=OnlineRetailSchema.INITIAL_SCHEMA,
)

# extract data
raw_rdd: RDD = rtp.extract()

# transform data
raw_df: DataFrame = rtp.transform_online_retail(raw_rdd)

# load data to S3 in parquet format
rtp.load_online_retail(raw_df)

# stop spark session
default_spark_session.stop()




