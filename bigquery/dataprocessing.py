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
    SELECT d1.*, d2.lat, d2.lon FROM ( 
        SELECT stn, wban, year, mo, da
        , IF( sndp = 999.9, null, sndp) sndp 
        , IF( prcp = 99.99, null, prcp) prcp
        , IF( temp = 9999.9, null, temp) temp
        , IF( CAST(wdsp as FLOAT64) = 999.9, null, wdsp) wdsp
        FROM `bigquery-public-data.noaa_gsod.gsod2018`
        WHERE stn = '725060' 
        UNION ALL
        SELECT stn, wban, year, mo, da
        , IF( sndp = 999.9, null, sndp) sndp 
        , IF( prcp = 99.99, null, prcp) prcp
        , IF( temp = 9999.9, null, temp) temp
        , IF( CAST(wdsp as FLOAT64) = 999.9, null, wdsp) wdsp
        FROM `bigquery-public-data.noaa_gsod.gsod2017`
        WHERE stn = '725060' 
        UNION ALL
        SELECT stn, wban, year, mo, da
        , IF( sndp = 999.9, null, sndp) sndp 
        , IF( prcp = 99.99, null, prcp) prcp
        , IF( temp = 9999.9, null, temp) temp
        , IF( CAST(wdsp as FLOAT64) = 999.9, null, wdsp) wdsp
        FROM `bigquery-public-data.noaa_gsod.gsod2016`
        WHERE stn = '725060' 
        UNION ALL
        SELECT stn, wban, year, mo, da
        , IF( sndp = 999.9, null, sndp) sndp 
        , IF( prcp = 99.99, null, prcp) prcp
        , IF( temp = 9999.9, null, temp) temp
        , IF( CAST(wdsp as FLOAT64) = 999.9, null, wdsp) wdsp
        FROM `bigquery-public-data.noaa_gsod.gsod2015`
        WHERE stn = '725060'
    ) AS d1 
    LEFT JOIN `bigquery-public-data.noaa_gsod.stations` d2 
    ON d1.wban = d2.wban  
    """

queryVal1b = """
    SELECT DISTINCT wban, lat, lon
    FROM `bigquery-public-data.noaa_gsod.stations`
    WHERE wban = '14756'
    """

# 2. bike data: no. of rentals per day, by station
queryVal2a = """
    SELECT start_station_id 
        , DATE(starttime) as date
        , EXTRACT(DAY FROM DATE(starttime)) as da
        , EXTRACT(MONTH FROM DATE(starttime)) as mo
        , EXTRACT(YEAR FROM DATE(starttime)) as year
        , usertype
        , start_station_name 
        , start_station_latitude
        , start_station_longitude
        , sum(tripduration) as totalSecondsRented
        , sum(1) as rentCounter
    FROM `bigquery-public-data.new_york_citibike.citibike_trips` 
    WHERE start_station_id IS NOT NULL
    GROUP BY start_station_id
        , date
        , da
        , mo
        , year
        , usertype
        , start_station_name
        , start_station_latitude
        , start_station_longitude  
    ORDER BY start_station_id, date
    """

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
    SELECT station_id 
        , name
        , short_name 
        , latitude 
        , longitude 
        , region_id 
        , rental_methods
        , capacity
        , num_bikes_available 
        , num_bikes_disabled
        , num_docks_available
        , num_docks_disabled
        , is_installed
        , is_renting
        , is_returning
        , last_reported
        , eightd_has_available_keys
        , eightd_has_key_dispenser
    FROM `bigquery-public-data.new_york_citibike.citibike_stations`
    """

# 4. population data: geoid, with population, age range
queryVal3 = """
	SELECT zipcode
  		, geo_id
  		, minimum_age
  		, maximum_age
  		, gender
  		, population 
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
    destinationUri = 'gs://{}/tables/{}/{}'.format(BUCKET_NAME, outdir, 'output.csv')
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
    prepareBigQueryData(client, queryVal1a, "noaa_gsod_extract")
    prepareBigQueryData(client, queryVal1b, "noaa_metadata_extract")
    prepareBigQueryData(client, queryVal2a, "citibike_trips_extract")
    prepareBigQueryData(client, queryVal2b, "citibike_path_extract")
    prepareBigQueryData(client, queryVal2c, "citibike_statmeta_extract")
    prepareBigQueryData(client, queryVal3, "census_pop_extract")

    # save to GCS
    saveBigQueryDataToGCP(client, "weatherdata", "noaa_gsod_extract")
    saveBigQueryDataToGCP(client, "weathermeta", "noaa_metadata_extract")
    saveBigQueryDataToGCP(client, "bikedata", "citibike_trips_extract")
    saveBigQueryDataToGCP(client, "bikepathdata", "citibike_path_extract")
    saveBigQueryDataToGCP(client, "statmetadata", "citibike_statmeta_extract")
    saveBigQueryDataToGCP(client, "censusdata", "census_pop_extract")
