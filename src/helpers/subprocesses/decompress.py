import subprocess


def decompress(file):
    p = subprocess.Popen(
        [f'lzop -d {file}'],
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )

    p.communicate()