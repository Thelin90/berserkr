import subprocess
from io import StringIO


def decompress(file):
    pipe = subprocess.Popen([f'{file} | lzo -d --stdout'], stdout=subprocess.PIPE, shell=True)
    helper = StringIO()
    helper.write(pipe.stdout.read())
    helper.seek(0)
