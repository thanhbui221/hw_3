CREATE EXTERNAL TABLE `angelic-throne-411816.de.green_taxi_nyc`
  OPTIONS (
    format ="PARQUET",
    uris = ['gs://green_taxi_bt1/*.parquet']
    );

select count(*) from `angelic-throne-411816.de.green_taxi_nyc`;

SELECT
  table_id,
  ROUND(size_bytes / POW(10, 6), 2) AS size_mb
FROM
  `angelic-throne-411816.de.__TABLES__`
WHERE
  table_id = 'green_taxi_nyc';

select count(*)
from `angelic-throne-411816.de.green_taxi_nyc`
where fare_amount = 0;

create or replace table `angelic-throne-411816.de.green_taxi_nyc_non_partitioned`
as select * from `angelic-throne-411816.de.green_taxi_nyc`;

SELECT
  table_id,
  size_bytes
FROM
  `angelic-throne-411816.de.__TABLES__`
WHERE
  table_id = 'green_taxi_nyc_non_partitioned';


create or replace table `angelic-throne-411816.de.green_taxi_nyc_partitioned_clusterd`
partition by date(lpep_pickup_datetime)
cluster by PUlocationID
as select * from `angelic-throne-411816.de.green_taxi_nyc`;  

SELECT
  table_id,
  size_bytes
FROM
  `angelic-throne-411816.de.__TABLES__`
WHERE
  table_id = 'green_taxi_nyc_partitioned_clusterd';