CREATE EXTERNAL TABLE IF NOT EXISTS bikedata (
    start_station_id INT
   ,date STRING
   ,day INT
   ,month INT
   ,year INT
   ,usertype STRING
   ,start_station_name STRING
   ,start_station_latitude DOUBLE
   ,start_station_longitude DOUBLE
   ,totalSecondsRented INT
   ,rentCounter INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'gs://denolte-showcase-qlikbigdata/tables/bikedata'
tblproperties ("skip.header.line.count"="1");

CREATE EXTERNAL TABLE IF NOT EXISTS bikepathdata (
    tripduration INT
   ,starttime STRING
   ,stoptime STRING
   ,start_station_id INT
   ,start_station_name STRING
   ,start_station_latitude DOUBLE
   ,start_station_longitude DOUBLE
   ,end_station_id INT
   ,end_station_name STRING
   ,end_station_latitude DOUBLE
   ,end_station_longitude DOUBLE
   ,bikeid INT
   ,usertype STRING
   ,birth_year INT
   ,gender STRING
   ,customer_plan STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'gs://denolte-showcase-qlikbigdata/tables/bikepathdata'
tblproperties ("skip.header.line.count"="1");

CREATE EXTERNAL TABLE IF NOT EXISTS weatherdata (
    station INT
   ,year INT
   ,month INT
   ,day INT
   ,sndp DOUBLE
   ,prcp DOUBLE
   ,temp DOUBLE
   ,wdsp DOUBLE
   ,lat DOUBLE
   ,lon DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'gs://denolte-showcase-qlikbigdata/tables/weatherdata'
tblproperties ("skip.header.line.count"="1");

CREATE EXTERNAL TABLE IF NOT EXISTS weathermeta (
    station INT
   ,lon DOUBLE
   ,lat DOUBLE
   ,postcode STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'gs://denolte-showcase-qlikbigdata/tables/weathermeta'
tblproperties ("skip.header.line.count"="1");

CREATE EXTERNAL TABLE IF NOT EXISTS censusdata (
    zipcode STRING
   ,geo_id STRING
   ,minimum_age INT
   ,maximum_age INT
   ,gender STRING
   ,population STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'gs://denolte-showcase-qlikbigdata/tables/censusdata'
tblproperties ("skip.header.line.count"="1");

CREATE EXTERNAL TABLE IF NOT EXISTS statmetadata (
    station_id INT
   ,name STRING
   ,short_name STRING
   ,latitude DOUBLE
   ,longitude DOUBLE
   ,region_id INT
   ,num_bikes_available INT
   ,is_renting BOOLEAN
   ,rental_methods STRING
   ,capacity INT
   ,num_bikes_disabled INT
   ,num_docks_available INT
   ,num_docks_disabled INT
   ,is_installed BOOLEAN
   ,is_returning BOOLEAN
   ,last_reported STRING
   ,eightd_has_available_keys BOOLEAN
   ,eightd_has_key_dispenser BOOLEAN
   ,postcode STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'gs://denolte-showcase-qlikbigdata/tables/statmetadata'
tblproperties ("skip.header.line.count"="1");
