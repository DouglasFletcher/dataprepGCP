## dependencies
#pip install -r requirements.txt

export DATASET="DATAPROCESS"
export BUCKET="qwiklabs-gcp-f0ee0f40b36f26ad"
#export BUCKET="denolte-showcase-qlikbigdata"
export PROJECT="qwiklabs-gcp-f0ee0f40b36f26ad"
#export PROJECT="denolte-showcase-qlikbigdata"
export LOCATION="US"

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
echo y | bq rm $DATASET --force 

