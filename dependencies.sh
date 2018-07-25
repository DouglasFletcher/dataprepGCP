## dependencies
pip install -r requirements.txt

#BUCKET="denolte-showcase-qlikbigdata"
#PROJECT="denolte-showcase-qlikbigdata"
DATASET="DATAPROCESS"

BUCKET="qwiklabs-gcp-20478e656e2da2c9"
PROJECT="qwiklabs-gcp-20478e656e2da2c9"

echo "env variables set: BUCKET=$BUCKET PROJECT=$PROJECT DATASET=$DATASET" 

## clone repository --> move to deployment script
#git clone https://github.com/DouglasFletcher/dataprepGCP.git

## first delete files if exist
gsutil -m rm -rf gs://$BUCKET/dir_noaa/noaa_gsod_extract
gsutil -m rm -rf gs://$BUCKET/dir_bike/citibike_trips_extract

# set up datasets in bigquery
echo y | bq mk $DATASET

## run application
cd /dataprepGCP
python dataprocessing.py 

# remove datasets in bigquery
echo y | bq rm $DATASET 

