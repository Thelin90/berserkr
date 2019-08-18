import pytest

from typing import List
from unittest.mock import patch, MagicMock
from src.helpers.subprocesses.decompress import decompress_lzo

GLOB = 'src.helpers.subprocesses.decompress.glob'
OS = 'src.helpers.subprocesses.decompress.os'
SUBPROCESS_RUN = 'src.helpers.subprocesses.decompress.subprocess.run'


class TestLZODecompression(object):

    @patch(OS)
    @patch(GLOB)
    @pytest.mark.parametrize('output_from_decode, expected_res, fake_file', [(
        '1, 2, FAKE SOMETHING, 3, 12/1/2019 8:26, 4.4, 5, United Kingdom\n'
        '2, 3, FAKE SOMETHING, 4, 12/1/2019 8:27, 5.5, 6, United Kingdom\n',
        ['1, 2, FAKE SOMETHING, 3, 12/1/2019 8:26, 4.4, 5, United Kingdom',
         '2, 3, FAKE SOMETHING, 4, 12/1/2019 8:27, 5.5, 6, United Kingdom'],
        'fake-file.csv.lzo'
    )])
    def test_decompress_lzo_success_and_failure(
            self,
            os_mock,  # never use, just wanna mock it out
            glob_mock,  # never use, just wanna mock it out
            output_from_decode,
            expected_res,
            fake_file,
    ):
        with patch(SUBPROCESS_RUN) as result:
            filter_mock = MagicMock()
            filter_mock.filter.return_value = expected_res
            decompress_lzo.return_value = filter_mock

            # mock return value of stdout.decode()
            def decode(decode_format):
                """Mock function for decode

                :param decode_format: the input for 'utf-8', 'ascii' but never used in this
                mocked version
                :return: the expected output from the downloaded file
                """
                return output_from_decode

            decode_mock = MagicMock(decode=decode)
            std_out_mock = MagicMock(stdout=decode_mock)
            returncode_mock = result.return_value = std_out_mock
            returncode_mock.returncode = 0

            fake_rows = decompress_lzo(fake_file)

            assert result.call_count == 1
            assert isinstance(fake_rows, List)
            assert fake_rows == expected_res

            # test with a not accepted return code
            returncode_mock.returncode = 1

            # should raise error
            with pytest.raises(ValueError):
                decompress_lzo(fake_file)

            assert result.call_count == 2
