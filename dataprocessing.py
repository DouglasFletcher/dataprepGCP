#!/usr/bin/env python

from google.cloud import bigquery

##########################################################################
# Process
    # 1. save relavant data to big query staging area
        # a. extract weather data with relavant stations
        # b. extract bike count by day
    # 2. export saved data in staging area go GCS
##########################################################################

## Documentation: so far..

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

# c. population data - does not make sense - yearly agregates

##########################################################################

# globals:
BUCKET_NAME = "BUCKET_NAME" # <-- dynamic?
PROJECT_ID= "SET_PROJECT_ID" # <-- define: static val?
DATASET_ID = BUCKET_NAME

# queries
# 1. weather data: precipitation by day in new-york
queryVal1 = """
    SELECT stn, year, mo, da, temp, prcp 
    FROM [bigquery-public-data:noaa_gsod.gsod2018]
        , [bigquery-public-data:noaa_gsod.gsod2017]
        , [bigquery-public-data:noaa_gsod.gsod2016]
        , [bigquery-public-data:noaa_gsod.gsod2015]
        , [bigquery-public-data:noaa_gsod.gsod2014]
        , [bigquery-public-data:noaa_gsod.gsod2013]
    WHERE stn = '725060'    
    """
# 2. bike data: no. of rentals per day, by station
queryVal2 = """
    SELECT start_station_id 
        , DATE(starttime) as date
        , DAY(starttime) as da
        , MONTH(starttime) as mo
        , YEAR(starttime) as year
        , start_station_name 
        , start_station_latitude
        , start_station_longitude
        , sum(1) as rentCounter
    FROM [bigquery-public-data:new_york_citibike.citibike_trips] 
    WHERE start_station_id IS NOT NULL
    GROUP BY start_station_id
        , date
        , da
        , mo
        , year
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
    tableRef = client.dataset(DATASET_ID).table(saveTableLoc)
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
    destinationUri = 'gs://{}/{}/{}'.format(BUCKET_NAME, outdir, 'output.*')
    datasetRef = client.dataset(DATASET_ID, project=PROJECT_ID)
    tableRef = datasetRef.table(tableId)
    # extract to GCS
    extractJob = client.extract_table(
        tableRef,
        destinationUri,
        location='US'
    )  
    extract_job.result()  
    print('Exported {}:{}.{} to {}'.format(PROJECT_ID, DATASET_ID, tableId, destinationUri))


# main 
if __name__ == '__main__':
    
    # __init__ client
    client = bigquery.Client()

    # save to table bigquery: note- delete tables if exist before saving?    
    queryRelavantData(client, queryVal1, "noaa_gsod_extract")
    queryRelavantData(client, queryVal2, "citibike_trips_extract")

    # save to GCS
    saveBigQueryDataToGCP(client, "dir_noaa", "noaa_gsod_extract")
    saveBigQueryDataToGCP(client, "dir_bike", "citibike_trips_extract")
