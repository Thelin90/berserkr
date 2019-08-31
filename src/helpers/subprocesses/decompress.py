#!/usr/bin/env python
import subprocess
import glob
import os
import re

from typing import List


def decompress_lzo(file) -> List[str]:
    """Decompress lzo file, see https://www.lzop.org/lzop_man.php

    :param file: file to decompress
    :return: the output from the decompressed file
    """

    # with run() no need to take care of communicate since
    # it is built in, nice!
    p = subprocess.run(
        f'lzop -cd {file}',
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    if p.returncode != 0:
        raise ValueError(f'{p.returncode}:{p.stderr}')
    else:

        # remove any lzo file on the executor host
        for fl in glob.glob("*.lzo"):
            os.remove(fl)

        output = p.stdout \
            .decode('utf-8') \

        # remove commas within qoutes `" some text,,, "`
        output = re.sub(r'"[^"]*"', lambda m: m.group(0).replace(',', ''), output)

        # remove redundant `" "`, split per new row to become a list
        output = output.replace('"', '').strip().split('\n')

        return output
