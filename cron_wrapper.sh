#!/bin/bash

cd /backup/batch_jobs/aqicn_air_quality/

source .secret.sh
source .venv/bin/activate

./fetch_aqicn_data.py
