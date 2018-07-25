## dependencies
pip install -r requirements.txt

BUCKET="denolte-showcase-qlikbigdata"
PROJECT="denolte-showcase-qlikbigdata"
DATASET="DATAPROCESS"

echo "env variables set: BUCKET= $BUCKET PROJECT= $PROJECT DATASET=$DATASET" 

## first delete files if exist
gsutil -m rm -rf gs://$BUCKET/dir_noaa/noaa_gsod_extract
gsutil -m rm -rf gs://$BUCKET/dir_bike/citibike_trips_extract

## process to clear bigquery?

## run python script: note - pass global variables here? BUCKET_NAME, PROJECT_ID
## clone repository --> move to deployment script
git clone https://github.com/DouglasFletcher/dataprepGCP.git

# set up datasets in bigquery
echo y | bq mk DATAPROCESS

## run application
cd ../dataprepGCP
python dataprocessing.py 

# remove datasets in bigquery
echo y | bq rm DATAPROCESS 

