#!/bin/bash

## geocode test
export BUCKET="denolte-showcase-qlikbigdata"
export PROJECT="denolte-showcase-qlikbigdata"

# get working data
gsutil cp gs://$BUCKET/tables/weatherdata/output.csv ./weatherdata/output.csv
gsutil cp gs://$BUCKET/tables/statmetadata/output.csv ./statmetadata/output.csv

# geocode files
python geocoding.py

# save back to cloud
gsutil cp ./weatherdata/output_geocoded.csv gs://$BUCKET/tables/weatherdata/output_geocoded.csv
gsutil cp ./statmetadata/output_geocoded.csv gs://$BUCKET/tables/statmetadata/output_geocoded.csv
