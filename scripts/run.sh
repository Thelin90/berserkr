#!/usr/bin/env bash

python3.6 -m pip install -r scripts/requirements.txt
spark-submit src/main.py