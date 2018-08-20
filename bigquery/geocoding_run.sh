#!/bin/bash

## geocode test
export BUCKET="denolte-showcase-qlikbigdata"
export PROJECT="denolte-showcase-qlikbigdata"

# remove geocoded files
gsutil -m rm -rf gs://$BUCKET/tables/noaa_gsod_stations_geocoded/output.csv
gsutil -m rm -rf gs://$BUCKET/tables/citybike_stations_geocoded/output.csv

# get working data
gsutil cp gs://$BUCKET/tables/noaa_gsod_stations/output.csv ./noaa_gsod_stations/output.csv
gsutil cp gs://$BUCKET/tables/citybike_stations/output.csv ./citybike_stations/output.csv

# geocode files
python geocoding.py

# save back to cloud
gsutil cp ./noaa_gsod_stations/output_geocoded.csv gs://$BUCKET/tables/noaa_gsod_stations/output.csv
gsutil cp ./citybike_stations/output_geocoded.csv gs://$BUCKET/tables/citybike_stations/output.csv

# Clean working directory
rm -rf ./noaa_gsod_stations
rm -rf ./citybike_stations
