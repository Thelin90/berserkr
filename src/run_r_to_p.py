from environs import Env

from pyspark import RDD
from pyspark.sql import DataFrame
from src.spark_session import InitSpark
from src.apps.raw_to_parquet import RawToParquet
from src.modules.schemas import OnlineRetailSchema as retail_schema

env: Env = Env()
env.read_env()


# Initialise SparkSession
init_spark_session = InitSpark("RawToParquet", "spark-warehouse")
default_spark_session = init_spark_session.spark_init()
default_spark_session.sparkContext.setLogLevel("WARN")
# Enable Arrow-based columnar data transfers
default_spark_session.conf.set("spark.sql.execution.arrow.enabled", "true")

s3_bucket = env('RAW_DATA_ONLINE_RETAIL_S3_BUCKET')
endpoint_url = env('ENDPOINT_URL')
aws_access_key_id = env('AWS_ACCESS_KEY_ID')
aws_secret_access_key = env('AWS_SECRET_ACCESS_KEY')
signature_version = env('SIGNATURE_VERSION')

rtp = RawToParquet(
    spark_session=default_spark_session,
    s3_bucket=s3_bucket,
    endpoint_url=endpoint_url,
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    signature_version=signature_version,
    schema=retail_schema.INITIAL_SCHEMA,
)

# extract data
rtp.extract()

# transform data
df: DataFrame = rtp.transform_online_retail()

# load data
rtp.load_online_retail(df)



