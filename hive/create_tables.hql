set hive.support.sql11.reserved.keywords=false;

CREATE EXTERNAL TABLE IF NOT EXISTS citybike_stations (
    station_id INT
   ,name STRING
   ,short_name STRING
   ,latitude FLOAT
   ,longitude FLOAT
   ,region_id INT
   ,rental_methods STRING
   ,capacity INT
   ,num_bikes_available INT
   ,num_bikes_disabled INT
   ,num_docks_available INT
   ,num_docks_disabled INT
   ,is_installed BOOLEAN
   ,is_renting BOOLEAN
   ,is_returning BOOLEAN
   ,last_reported STRING
   ,eightd_has_available_keys BOOLEAN
   ,eightd_has_key_dispenser BOOLEAN
   ,postcode STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'gs://denolte-showcase-qlikbigdata/tables/citybike_stations'
tblproperties ("skip.header.line.count"="1");

CREATE EXTERNAL TABLE IF NOT EXISTS citybike_trips (
    tripduration INT
   ,starttime STRING
   ,stoptime STRING
   ,start_station_id INT
   ,start_station_name STRING
   ,start_station_latitude FLOAT
   ,start_station_longitude FLOAT
   ,end_station_id INT
   ,end_station_name STRING
   ,end_station_latitude FLOAT
   ,end_station_longitude FLOAT
   ,bikeid INT
   ,usertype STRING
   ,birth_year INT
   ,gender STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'gs://denolte-showcase-qlikbigdata/tables/citybike_trips'
tblproperties ("skip.header.line.count"="1");

CREATE EXTERNAL TABLE IF NOT EXISTS noaa_gsod (
    stn STRING
   ,wban STRING
   ,year STRING
   ,mo STRING
   ,da STRING
   ,temp FLOAT
   ,count_temp INT
   ,dewp FLOAT
   ,count_dewp INT
   ,slp FLOAT
   ,count_slp INT
   ,stp FLOAT
   ,count_stp INT
   ,visib FLOAT
   ,count_visib INT
   ,wdsp STRING
   ,count_wdsp STRING
   ,mxpsd STRING
   ,gust FLOAT
   ,max FLOAT
   ,flag_max STRING
   ,min FLOAT
   ,flag_min STRING
   ,prcp FLOAT
   ,flag_prcp STRING
   ,sndp FLOAT
   ,fog STRING
   ,rain_drizzle STRING
   ,snow_ice_pellets STRING
   ,hail STRING
   ,thunder STRING
   ,tornado_funnel_cloud STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'gs://denolte-showcase-qlikbigdata/tables/noaa_gsod'
tblproperties ("skip.header.line.count"="1");

CREATE EXTERNAL TABLE IF NOT EXISTS noaa_gsod_stations (
    usaf STRING
   ,wban STRING
   ,name STRING
   ,country STRING
   ,state STRING
   ,call STRING
   ,lat FLOAT
   ,lon FLOAT
   ,elev STRING
   ,begin STRING
   ,`end` STRING
   ,postcode STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'gs://denolte-showcase-qlikbigdata/tables/noaa_gsod_stations'
tblproperties ("skip.header.line.count"="1");

CREATE EXTERNAL TABLE IF NOT EXISTS population_by_zip_2010 (
    zipcode STRING
   ,geo_id STRING
   ,minimum_age INT
   ,maximum_age INT
   ,gender STRING
   ,population INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'gs://denolte-showcase-qlikbigdata/tables/population_by_zip_2010'
tblproperties ("skip.header.line.count"="1");
