#!/bin/bash

## geocode test
#gcloud dataproc jobs submit pyspark --cluster master --region europe-west3 geocoding.py

export BUCKET="denolte-showcase-qlikbigdata"
export PROJECT="denolte-showcase-qlikbigdata"

python geocoding.py