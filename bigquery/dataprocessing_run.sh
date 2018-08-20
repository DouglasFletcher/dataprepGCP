#!/bin/bash

## environment variables
export DATASET="DATAPROCESS"
#export BUCKET="qwiklabs-gcp-f0ee0f40b36f26ad"
export BUCKET="denolte-showcase-qlikbigdata"
#export PROJECT="qwiklabs-gcp-f0ee0f40b36f26ad"
export PROJECT="denolte-showcase-qlikbigdata"
export LOCATION="US"
echo "env variables set: BUCKET=$BUCKET PROJECT=$PROJECT DATASET=$DATASET LOCATION=$LOCATION" 

## delete existing data in cloud storage
gsutil -m rm -rf gs://$BUCKET/tables/noaa_gsod/output.csv
gsutil -m rm -rf gs://$BUCKET/tables/noaa_gsod_stations/output.csv
gsutil -m rm -rf gs://$BUCKET/tables/citybike_trips/output.csv
# gsutil -m rm -rf gs://$BUCKET/tables/bikepathdata/output.csv
gsutil -m rm -rf gs://$BUCKET/tables/citybike_stations/output.csv
gsutil -m rm -rf gs://$BUCKET/tables/population_by_zip_2010/output.csv

## set up datasets in bigquery
echo y | bq mk $DATASET

## run application
python dataprocessing.py 

## remove datasets in bigquery
echo y | bq rm -rf $DATASET
