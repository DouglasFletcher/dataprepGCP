## dependencies
pip install -r requirements.txt

export DATASET="DATAPROCESS"
export BUCKET="qwiklabs-gcp-20478e656e2da2c9"
export PROJECT="qwiklabs-gcp-20478e656e2da2c9"
export LOCATION="US"
#export BUCKET="denolte-showcase-qlikbigdata"
#export PROJECT="denolte-showcase-qlikbigdata"

echo "env variables set: BUCKET=$BUCKET PROJECT=$PROJECT DATASET=$DATASET LOCATION=$LOCATION" 

## clone repository --> move to deployment script
#git clone https://github.com/DouglasFletcher/dataprepGCP.git

## first delete files if exist
gsutil -m rm -rf gs://$BUCKET/tmp/dir_noaa/output.csv
gsutil -m rm -rf gs://$BUCKET/tmp/dir_bike/output.csv

# set up datasets in bigquery
echo y | bq mk $DATASET

## run application
cd /dataprepGCP
python dataprocessing.py 

# remove datasets in bigquery
echo y | bq rm $DATASET 

