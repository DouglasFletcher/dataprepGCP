#!/bin/bash

## geocode test
export BUCKET="denolte-showcase-qlikbigdata"
export PROJECT="denolte-showcase-qlikbigdata"

# remove geocoded files
gsutil -m rm -rf gs://$BUCKET/tables/weathermeta/output_geocoded.csv
gsutil -m rm -rf gs://$BUCKET/tables/statmetadata/output_geocoded.csv

# get working data
gsutil cp gs://$BUCKET/tables/weathermeta/output.csv ./weathermeta/output.csv
gsutil cp gs://$BUCKET/tables/statmetadata/output.csv ./statmetadata/output.csv

# geocode files
python geocoding.py

# save back to cloud
gsutil cp ./weathermeta/output_geocoded.csv gs://$BUCKET/tables/weathermeta/output_geocoded.csv
gsutil cp ./statmetadata/output_geocoded.csv gs://$BUCKET/tables/statmetadata/output_geocoded.csv
