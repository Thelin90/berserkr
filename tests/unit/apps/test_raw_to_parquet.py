from unittest.mock import MagicMock, call, patch

import pytest
from pyspark import RDD
from pyspark.sql import DataFrame, SparkSession
from src.apps.raw_to_parquet import RawToParquet
from src.helpers.online_retail.transform import transform_online_retail
from src.modules.schemas import OnlineRetailSchema

R2P = 'src.apps.raw_to_parquet'
DIST_READ_S3 = 'src.helpers.aws.distributed_read_s3'
S3 = 'src.helpers.aws.s3_specific'


@pytest.fixture(scope='session')
def spark_session():
    spark_session = SparkSession.builder \
        .master("local[2]") \
        .appName("unit-test-berserkr").getOrCreate()
    return spark_session


@pytest.fixture(scope='session')
def rtp_csv(spark_session):
    rtp_csv = RawToParquet(
        spark_session=spark_session,
        raw_s3_bucket='fake-bucket',
        parquet_s3_bucket='fake-parquet-bucket',
        aws_endpoint_url='fake-endpoint',
        aws_access_key_id='fake-access-key',
        aws_secret_access_key='fake-secret-key',
        signature_version='fake-signature-version',
        schema=OnlineRetailSchema.INITIAL_SCHEMA,
        transform_call=transform_online_retail,
        raw_format='csv',
    )
    return rtp_csv


@pytest.fixture()
def expected_raw_rdd_from_csv(spark_session):
    expected_rdd = spark_session.sparkContext.parallelize(
        ['fake-data1', 'fake-data2']).map(lambda x: [x])
    return expected_rdd


@pytest.fixture()
def mock_dist_csv():
    fake_csv_data = ['header-col-num', 'fake-data1', 'fake-data2']
    with patch(f'{DIST_READ_S3}.distributed_fetch',
               side_effect=[fake_csv_data]) as mock_dist:
        yield mock_dist


@pytest.fixture()
def mock_bucket():
    with patch(f'{DIST_READ_S3}.get_bucket_files',
               side_effect=[['fake-file']]) as mock_bucket:
        yield mock_bucket


class TestRawParquetCSV(object):
    def test_extract_csv(self, rtp_csv, mock_dist_csv, mock_bucket,
                         expected_raw_rdd_from_csv,
                         spark_session: SparkSession):
        fake_raw_rdd = rtp_csv.extract()

        assert isinstance(fake_raw_rdd, RDD)
        assert fake_raw_rdd.collect() == expected_raw_rdd_from_csv.collect()

        spark_session.stop()

    def test_transform_csv(self, expected_raw_rdd_from_csv,
                           spark_session: SparkSession):
        fake_dataframe = rtp_csv.transform(expected_raw_rdd_from_csv)

        assert isinstance(fake_dataframe, DataFrame)
        assert fake_dataframe.count == 2

        spark_session.stop()
