## dependencies
pip install -r requirements.txt

## first delete files if exist
gsutil -m rm -rf gs://$BUCKET/dir_noaa/noaa_gsod_extract
gsutil -m rm -rf gs://$BUCKET/dir_bike/citibike_trips_extract

## process to clear bigquery?

## run python script: note - pass global variables here? BUCKET_NAME, PROJECT_ID
## clone repository --> move to deployment script
rm -rf ../dataprepGCP
git clone https://github.com/DouglasFletcher/dataprepGCP.git

## run application
cd ../dataprepGCP
python dataprocessing.py 

