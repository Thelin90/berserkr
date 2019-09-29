#!/usr/bin/env python
import subprocess
import glob
import os
import re

from typing import List


def lzop_subprocess(file):
    return subprocess.run(
        f'lzop -cd {file}',
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


def decompress_json(file) -> List[str]:
    pass


def decompress_csv(file) -> List[str]:
    """Decompress lzo file, see https://www.lzop.org/lzop_man.php

    :param file: file to decompress
    :return: the output from the decompressed file
    """

    p = lzop_subprocess(file)

    if p.returncode != 0:
        raise ValueError(f'{p.returncode}:{p.stderr}')
    else:

        # remove any lzo file on the executor host
        for fl in glob.glob("*.lzo"):
            os.remove(fl)

        # decode output from byte to str
        output = p.stdout.decode('utf-8')

        # "[^ ]" = Matches a single character that is not contained within the brackets.
        # "[^ "]" = Matches everything within `"`. For example: '"HELLO",HELLO' -> "HELLO"
        #
        # * = Matches the preceding element zero or more times
        #
        # "[^"]*" = Matches a single preceding element zero or more times in this case
        # in combination with .group(0).replace(',', '').
        # Where .group(0) locates the whole matched expression.
        #
        # Hence: "HELLO,,," -> "HELLO,,,".replace(',', '') -> "HELLO"
        output = re.sub(r'"[^"]*"', lambda m: m.group(0).replace(',', ''), output)

        # remove redundant `" "`, split per new row to become a list
        output = output.replace('"', '').strip().split('\n')

        return output
