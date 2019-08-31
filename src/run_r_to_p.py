from environs import Env

from src.spark_session import InitSpark
from src.apps.raw_to_parquet import RawToParquet
from src.modules.schemas import OnlineRetailSchema

env: Env = Env()
env.read_env()

# read env variables
raw_s3_bucket = env('RAW_DATA_ONLINE_RETAIL_S3_BUCKET')
parquet_s3_bucket = env('PARQUET_DATA_ONLINE_RETAIL_S3_BUCKET')
endpoint_url = env('ENDPOINT_URL')
aws_access_key_id = env('AWS_ACCESS_KEY_ID')
aws_secret_access_key = env('AWS_SECRET_ACCESS_KEY')
signature_version = env('SIGNATURE_VERSION')


# Initialise SparkSession
init_spark_session = InitSpark(
    "RawToParquet",
    "spark-warehouse",
    endpoint_url=endpoint_url,
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key
)

default_spark_session = init_spark_session.spark_init()

rtp = RawToParquet(
    spark_session=default_spark_session,
    raw_s3_bucket=raw_s3_bucket,
    parquet_s3_bucket=parquet_s3_bucket,
    endpoint_url=endpoint_url,
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    signature_version=signature_version,
    schema=OnlineRetailSchema.INITIAL_SCHEMA,
)

# extract data
rtp.extract()

# transform data
rtp.transform_online_retail()

# load data
rtp.load_online_retail()



