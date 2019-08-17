import subprocess
import glob
import os


from typing import List


def decompress_lzo(file) -> List[List[str]]:
    """Decompress lzo file, see https://www.lzop.org/lzop_man.php

    :param file: file to decompress
    :return: the output from the decompressed file
    """

    # with run() no need to take care of communicate since
    # it is built in, nice!
    p = subprocess.run(
        [f'lzop -cd {file}'],
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    if p.returncode != 0:
        raise ValueError(p.returncode)
    else:

        # remove any lzo file on the executor host
        for fl in glob.glob("*.lzo"):
            os.remove(fl)

        return [p.stdout.decode('utf-8')]
