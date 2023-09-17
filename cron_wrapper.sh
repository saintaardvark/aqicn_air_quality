#!/bin/bash

cd /backup/batch_jobs/aqicn_air_quality/

source .secret.sh
source .venv/bin/activate

./aqicn.py current --random-sleep 30
