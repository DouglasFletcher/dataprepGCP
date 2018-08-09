#!/bin/bash

## geocode test
export BUCKET="denolte-showcase-qlikbigdata"
export PROJECT="denolte-showcase-qlikbigdata"

# get working data
gsutil cp gs://$BUCKET/tables/weatherdata/output.csv ./weatherdata/output.csv
#gsutil cp $BUCKET/tables/statmetadata/output.csv ./statmetadata/output.csv
#gsutil cp $BUCKET/tables/bikedata/output.csv ./bikedata/output.csv

# geocode files
python geocoding.py

# save back to cloud
gsutil cp ./weatherdata/output_complete.csv gs://$BUCKET/tables/weatherdata/output_complete.csv
#gsutil cp ./statmetadata/output.csv $BUCKET/tables/statmetadata/output.csv
#gsutil cp ./bikedata/output.csv $BUCKET/tables/bikedata/output.csv
