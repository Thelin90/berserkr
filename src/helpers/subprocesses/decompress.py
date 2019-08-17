import subprocess


def decompress(file):
    p = subprocess.Popen(
        [f'lzop -d {file}'],
        shell=True,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )

    # Pipe input into first process
    result = p.communicate(p.stdout.read())

    p.stdout.close()

    return result
