import pytest

from unittest import TestCase
from unittest.mock import patch, MagicMock
from botocore.exceptions import ClientError
from src.helpers.aws.s3_specific import distributed_fetch

BOTO3_RESOURCE = 'src.helpers.aws.s3_specific.boto3.resource'


class TestS3Read(TestCase):

    def setUp(self) -> None:
        self.fake_filepath = 'fake_bucket/fakedata.csv'
        self.s3_bucket = 'fake_bucket'
        self.endpoint_url = 'http://0.0.0.0:0000'
        self.aws_access_key_id = 'fake-access-key-id'
        self.aws_secret_access_key = 'fake-secret-access-key'
        self.signature_version = 'fake-signature-version'

    def test_read_s3_file_sucess(self):
        distributed_fetch_mock = MagicMock()
        distributed_fetch.return_value = distributed_fetch_mock

        with patch(BOTO3_RESOURCE) as read_file:

            distributed_fetch(
                self.fake_filepath,
                self.s3_bucket,
                self.endpoint_url,
                self.aws_access_key_id,
                self.aws_secret_access_key,
                self.signature_version,
            )

        # 1 read should only occur when called
        self.assertEqual(read_file.call_count, 1)

    def test_read_s3_file_failure(self):
        with patch(BOTO3_RESOURCE) as read_file:

            error_response = {'Error': {'Code': '404'}}
            side_effect = ClientError(
                error_response, 'not found'
            )
            read_file.side_effect = side_effect
            with pytest.raises(ClientError):
                distributed_fetch(
                    self.fake_filepath,
                    self.s3_bucket,
                    self.endpoint_url,
                    self.aws_access_key_id,
                    self.aws_secret_access_key,
                    self.signature_version,
                )

            # read should fail and only been called once
            self.assertEqual(read_file.call_count, 1)
