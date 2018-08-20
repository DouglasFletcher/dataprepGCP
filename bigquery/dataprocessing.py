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
    # weather station values
    # --> relevant weather stations in new-york
    #SELECT usaf, wban, name, country, state, call  
    #FROM [bigquery-public-data:noaa_gsod.stations] 
    #WHERE REGEXP_MATCH(name, 'NEW YORK')
    # --> result {"usaf":"725060", "wban", "94728", "name", "NEW YORK CITY CENTRAL PARK"}
    
    # query1a: get weather data with geocoordinates merged from station metadata 

    # query1b: station metadata

# b. bike data - rentals
    # query 2a: aggreation of bikedata i.e. number of rentals per day

    # query 2b: bike data

    # query 2c: path of bike data

# c. population data - census
    # query3: census data

##########################################################################

# globals:
DATASET_ID = os.environ.get('DATASET') 
BUCKET_NAME = os.environ.get('BUCKET')
PROJECT_ID = os.environ.get('PROJECT')
LOCATION = os.environ.get('LOCATION')

# queries
# 1. weather data: precipitation by day in new-york
queryVal1a = """
    SELECT * FROM ( 
        SELECT
          stn
        , wban
        , year
        , mo
        , da
        , IF( temp = 9999.9, null, temp) temp
        , count_temp
        , dewp
        , count_dewp
        , slp
        , count_slp
        , stp
        , count_stp
        , visib
        , count_visib
        , IF( CAST(wdsp as FLOAT64) = 999.9, null, wdsp) wdsp
        , count_wdsp
        , mxpsd
        , gust
        , max
        , flag_max
        , min
        , flag_min
        , IF( prcp = 99.99, null, prcp) prcp
        , flag_prcp
        , IF( sndp = 999.9, null, sndp) sndp
        , fog
        , rain_drizzle
        , snow_ice_pellets
        , hail
        , thunder
        , tornado_funnel_cloud
        FROM `bigquery-public-data.noaa_gsod.gsod2018`
        UNION ALL
        SELECT 
          stn
        , wban
        , year
        , mo
        , da
        , IF( temp = 9999.9, null, temp) temp
        , count_temp
        , dewp
        , count_dewp
        , slp
        , count_slp
        , stp
        , count_stp
        , visib
        , count_visib
        , IF( CAST(wdsp as FLOAT64) = 999.9, null, wdsp) wdsp
        , count_wdsp
        , mxpsd
        , gust
        , max
        , flag_max
        , min
        , flag_min
        , IF( prcp = 99.99, null, prcp) prcp
        , flag_prcp
        , IF( sndp = 999.9, null, sndp) sndp
        , fog
        , rain_drizzle
        , snow_ice_pellets
        , hail
        , thunder
        , tornado_funnel_cloud
     FROM `bigquery-public-data.noaa_gsod.gsod2017`
        UNION ALL
        SELECT
          stn
        , wban
        , year
        , mo
        , da
        , IF( temp = 9999.9, null, temp) temp
        , count_temp
        , dewp
        , count_dewp
        , slp
        , count_slp
        , stp
        , count_stp
        , visib
        , count_visib
        , IF( CAST(wdsp as FLOAT64) = 999.9, null, wdsp) wdsp
        , count_wdsp
        , mxpsd
        , gust
        , max
        , flag_max
        , min
        , flag_min
        , IF( prcp = 99.99, null, prcp) prcp
        , flag_prcp
        , IF( sndp = 999.9, null, sndp) sndp
        , fog
        , rain_drizzle
        , snow_ice_pellets
        , hail
        , thunder
        , tornado_funnel_cloud
        FROM `bigquery-public-data.noaa_gsod.gsod2016`
        UNION ALL
        SELECT
          stn
        , wban
        , year
        , mo
        , da
        , IF( temp = 9999.9, null, temp) temp
        , count_temp
        , dewp
        , count_dewp
        , slp
        , count_slp
        , stp
        , count_stp
        , visib
        , count_visib
        , IF( CAST(wdsp as FLOAT64) = 999.9, null, wdsp) wdsp
        , count_wdsp
        , mxpsd
        , gust
        , max
        , flag_max
        , min
        , flag_min
        , IF( prcp = 99.99, null, prcp) prcp
        , flag_prcp
        , IF( sndp = 999.9, null, sndp) sndp
        , fog
        , rain_drizzle
        , snow_ice_pellets
        , hail
        , thunder
        , tornado_funnel_cloud
        FROM `bigquery-public-data.noaa_gsod.gsod2015`
    )
    """

queryVal1b = """
    SELECT *
    FROM `bigquery-public-data.noaa_gsod.stations`
    WHERE wban = '14756'
    """

# 2. bike data: no. of rentals per day, by station
queryVal2a = """
    SELECT * 
    FROM `bigquery-public-data.new_york_citibike.citibike_trips` 
    """

# Not used at the moment
queryVal2b = """
    SELECT
        tripduration
        , starttime
        , stoptime
        , start_station_id
        , start_station_name
        , start_station_latitude
        , start_station_longitude
        , end_station_id
        , end_station_name
        , end_station_latitude
        , end_station_longitude
        , bikeid
        , usertype
        , birth_year
        , gender
        , customer_plan
    FROM `bigquery-public-data.new_york_citibike.citibike_trips`
    LIMIT 1000000
    """

# 3. bike station metadata (e.g. possible rentals)
queryVal2c = """
    SELECT * 
    FROM `bigquery-public-data.new_york_citibike.citibike_stations`
    """

# 4. population data: geoid, with population, age range
queryVal3 = """
	SELECT *
	FROM `bigquery-public-data.census_bureau_usa.population_by_zip_2010`
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
    queryJob = client.query(queryVal, location=LOCATION, job_config=jobConfig)  
    queryJob.result() 
    print('Query results loaded to table {}'.format(tableRef.path)) 


def saveBigQueryDataToGCP(client, outdir, tableId):
    """
    save resultset from prepareBigQueryData to google cloud storage
    e.g. gs:/denolte-showcase-qlikbigdata/tables/bikedata/output.csv
    """
    # def save locations
    destinationUri = 'gs://{}/tables/{}'.format(BUCKET_NAME, outdir)
    datasetRef = client.dataset(DATASET_ID, project=PROJECT_ID)
    tableRef = datasetRef.table(tableId)
    # extract to GCS
    extractJob = client.extract_table(
        tableRef,
        destinationUri,
        location=LOCATION
    )  
    extractJob.result()  
    print('Exported {}:{}.{} to {}'.format(PROJECT_ID, DATASET_ID, tableId, destinationUri))


# main 
if __name__ == '__main__':
    
    # __init__ client
    client = bigquery.Client()

    # save to table bigquery: note- delete tables if exist before saving?    
    prepareBigQueryData(client, queryVal1a, "noaa_gsod")
    prepareBigQueryData(client, queryVal1b, "noaa_gsod_stations")
    prepareBigQueryData(client, queryVal2a, "citybike_trips")
    # prepareBigQueryData(client, queryVal2b, "citibike_path_extract")
    prepareBigQueryData(client, queryVal2c, "citybike_stations")
    prepareBigQueryData(client, queryVal3, "population_by_zip_2010")

    # save to GCS
    saveBigQueryDataToGCP(client, "noaa_gsod/output-*.csv", "noaa_gsod")
    saveBigQueryDataToGCP(client, "noaa_gsod_stations/output.csv", "noaa_gsod_stations")
    saveBigQueryDataToGCP(client, "citybike_trips/output-*.csv", "citybike_trips")
    # saveBigQueryDataToGCP(client, "bikepathdata", "citibike_path_extract")
    saveBigQueryDataToGCP(client, "citybike_stations/output.csv", "citybike_stations")
    saveBigQueryDataToGCP(client, "population_by_zip_2010/output.csv", "population_by_zip_2010")
