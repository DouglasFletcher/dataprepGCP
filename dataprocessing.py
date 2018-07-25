#!/usr/bin/env python

from google.cloud import bigquery
import os

##########################################################################
# Process
    # 1. save relavant data to big query staging area
        # a. extract weather data with relavant stations
        # b. extract bike count by day
    # 2. export saved data in staging area go GCS
##########################################################################

## Documentation: so far queries in bigdata webui..

# a. weather data
    # query 1: 
    # --> relevant weather stations in new-york
    #SELECT usaf, wban, name, country, state, call  
    #FROM [bigquery-public-data:noaa_gsod.stations] 
    #WHERE REGEXP_MATCH(name, 'NEW YORK')
    # --> result {"usaf":"725060", "wban", "94728", "name", "NEW YORK CITY CENTRAL PARK"}
    
    # query 2:
    #--> collect weather data for above station
    # SELECT stn, year, mo, da, temp, prcp 
    # FROM [bigquery-public-data:noaa_gsod.gsod2018]
    #   , [bigquery-public-data:noaa_gsod.gsod2017]
    #   , [bigquery-public-data:noaa_gsod.gsod2016]
    #   , [bigquery-public-data:noaa_gsod.gsod2015]
    # WHERE stn = '725060' <-- from above query

# b. bike data
    # query 1:
    # --> b. get a count of active rentals from stations
    # SELECT start_station_id, etc., , sum(1) as rentCounter
    #        FROM [bigquery-public-data:new_york_citibike.citibike_trips] 
    #        WHERE start_station_id IS NOT NULL
    #        GROUP BY start_station_id, etc.
    #        ORDER BY start_station_id, date

# c. population data - need to add query

##########################################################################

# globals:
DATASET_ID = os.environ.get('DATASET') 
BUCKET_NAME = os.environ.get('BUCKET')
PROJECT_ID = os.environ.get('PROJECT')

# queries
# 1. weather data: precipitation by day in new-york
queryVal1 = """
    SELECT stn, year, mo, da, temp, prcp 
    FROM `bigquery-public-data.noaa_gsod.gsod2018`
        , `bigquery-public-data.noaa_gsod.gsod2017`
        , `bigquery-public-data.noaa_gsod.gsod2016`
        , `bigquery-public-data.noaa_gsod.gsod2015`
        , `bigquery-public-data.noaa_gsod.gsod2014`
        , `bigquery-public-data.noaa_gsod.gsod2013`
    WHERE stn = '725060'    
    """

# 2. bike data: no. of rentals per day, by station
queryVal2 = """
    SELECT start_station_id 
        , starttime as date
        #, DATE(starttime) as date
        #, DAY(starttime) as da
        #, MONTH(starttime) as mo
        #, YEAR(starttime) as year
        , start_station_name 
        , start_station_latitude
        , start_station_longitude
        , sum(1) as rentCounter
    FROM `bigquery-public-data.new_york_citibike.citibike_trips` 
    WHERE start_station_id IS NOT NULL
    GROUP BY start_station_id
        , date
        #, da
        #, mo
        #, year
        , start_station_name
        , start_station_latitude
        , start_station_longitude  
    ORDER BY start_station_id, date
    """

# functions
def prepareBigQueryData(client, queryVal, tableId):
    """
    query public datasets and save back to table in bigquery
    """
    # save location: 
    jobConfig = bigquery.QueryJobConfig()
    tableRef = client.dataset(DATASET_ID).table(tableId)
    jobConfig.destination = tableRef
    # query result: location is set from global variable?
    queryJob = client.query(queryVal, location='US', job_config=jobConfig)  
    queryJob.result() 
    print('Query results loaded to table {}'.format(tableRef.path)) 


def saveBigQueryDataToGCP(client, outdir, tableId):
    """
    save resultset from prepareBigQueryData to google cloud storage
    """
    # def save locations
    destinationUri = 'gs://{}/tmp/{}/{}'.format(BUCKET_NAME, outdir, 'output.csv')
    datasetRef = client.dataset(DATASET_ID, project=PROJECT_ID)
    tableRef = datasetRef.table(tableId)
    # extract to GCS
    extractJob = client.extract_table(
        tableRef,
        destinationUri,
        location='US'
    )  
    extractJob.result()  
    print('Exported {}:{}.{} to {}'.format(PROJECT_ID, DATASET_ID, tableId, destinationUri))


# main 
if __name__ == '__main__':
    
    # __init__ client
    client = bigquery.Client()

    # save to table bigquery: note- delete tables if exist before saving?    
    prepareBigQueryData(client, queryVal1, "noaa_gsod_extract")
    #prepareBigQueryData(client, queryVal2, "citibike_trips_extract")

    # save to GCS
    saveBigQueryDataToGCP(client, "dir_noaa", "noaa_gsod_extract")
    #saveBigQueryDataToGCP(client, "dir_bike", "citibike_trips_extract")
