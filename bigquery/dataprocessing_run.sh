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
gsutil -m rm -rf gs://$BUCKET/tables/weatherdata/output.csv
gsutil -m rm -rf gs://$BUCKET/tables/bikedata/output.csv
gsutil -m rm -rf gs://$BUCKET/tables/bikepathdata/output.csv
gsutil -m rm -rf gs://$BUCKET/tables/statmetadata/output.csv
gsutil -m rm -rf gs://$BUCKET/tables/censusdata/output.csv

## set up datasets in bigquery
echo y | bq mk $DATASET

## run application
python dataprocessing.py 

## remove datasets in bigquery
echo y | bq rm -rf $DATASET