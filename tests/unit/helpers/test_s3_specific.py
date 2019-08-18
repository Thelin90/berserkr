import pytest

from unittest import TestCase
from unittest.mock import patch, MagicMock
from botocore.exceptions import ClientError
from src.helpers.aws.s3_specific import distributed_fetch, get_bucket_files

DECOMPRESS_LZO = 'src.helpers.aws.s3_specific.decompress_lzo'
BOTO3_RESOURCE = 'src.helpers.aws.s3_specific.boto3.resource'


class TestS3Read(TestCase):

    def setUp(self) -> None:
        self.fake_filepath: str = 'fake_bucket/fakedata.csv'
        self.fake_s3_bucket: str = 'fake_bucket'
        self.fake_endpoint_url: str = 'http://0.0.0.0:0000'
        self.fake_aws_access_key_id: str = 'fake-access-key-id'
        self.fake_aws_secret_access_key: str = 'fake-secret-access-key'
        self.fake_signature_version: str = 'fake-signature-version'
        self.error_response = {'Error': {'Code': '404'}}
        self.side_effect = ClientError(
            self.error_response, 'not found'
        )

    def test_get_bucket_files_success(self):
        with patch(BOTO3_RESOURCE) as bucket_files:
            get_bucket_files(
                endpoint_url=self.fake_endpoint_url,
                s3_bucket=self.fake_s3_bucket,
                aws_access_key_id=self.fake_aws_access_key_id,
                aws_secret_access_key=self.fake_aws_secret_access_key,
                signature_version=self.fake_signature_version,
            )
            self.assertEqual(bucket_files.call_count, 1)

    def test_get_bucket_files_failure(self):
        with patch(BOTO3_RESOURCE) as bucket_files:
            bucket_files.side_effect = self.side_effect
            with pytest.raises(ClientError):
                get_bucket_files(
                    endpoint_url=self.fake_endpoint_url,
                    s3_bucket=self.fake_s3_bucket,
                    aws_access_key_id=self.fake_aws_access_key_id,
                    aws_secret_access_key=self.fake_aws_secret_access_key,
                    signature_version=self.fake_signature_version,
                )
            self.assertEqual(bucket_files.call_count, 1)

    @patch(DECOMPRESS_LZO)
    def test_read_s3_file_sucess(self, decompress_lzo_mock):
        distributed_fetch_mock: MagicMock = MagicMock()
        distributed_fetch.return_value: MagicMock = distributed_fetch_mock

        with patch(BOTO3_RESOURCE) as read_file:
            distributed_fetch(
                filepath=self.fake_filepath,
                s3_bucket=self.fake_s3_bucket,
                endpoint_url=self.fake_endpoint_url,
                aws_access_key_id=self.fake_aws_access_key_id,
                aws_secret_access_key=self.fake_aws_secret_access_key,
                signature_version=self.fake_signature_version,
            )
        self.assertEqual(read_file.call_count, 1)

    def test_read_s3_file_failure(self):
        with patch(BOTO3_RESOURCE) as read_file:
            read_file.side_effect = self.side_effect
            with pytest.raises(ClientError):
                distributed_fetch(
                    filepath=self.fake_filepath,
                    s3_bucket=self.fake_s3_bucket,
                    endpoint_url=self.fake_endpoint_url,
                    aws_access_key_id=self.fake_aws_access_key_id,
                    aws_secret_access_key=self.fake_aws_secret_access_key,
                    signature_version=self.fake_signature_version,
                )
            self.assertEqual(read_file.call_count, 1)
