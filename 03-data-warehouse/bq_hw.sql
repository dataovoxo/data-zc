-- Question 1
CREATE OR REPLACE EXTERNAL TABLE `ny_taxi.greentaxi_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://keen-hangar-411922-terra-bucket/green/green_tripdata_2022-*.parquet']
);

SELECT count(*) 
FROM `ny_taxi.greentaxi_tripdata`;


--- Question 3
SELECT count(*) 
FROM `ny_taxi.greentaxi_tripdata`
WHERE fare_amount = 0;

--- Question 4, Partion and cluster tables and explore perfomance
--- Query performance shows improvement in data processed when table is partitioned
CREATE OR REPLACE TABLE ny_taxi.green_tripdata_non_partitoned AS
SELECT * FROM `ny_taxi.greentaxi_tripdata`;

CREATE OR REPLACE TABLE ny_taxi.green_tripdata_partitoned
PARTITION BY
  DATE(lpep_pickup_datetime) AS
SELECT * FROM ny_taxi.green_tripdata_non_partitoned;

SELECT *
FROM `ny_taxi.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'green_tripdata_partitoned'
ORDER BY total_rows DESC;


-- Impact of partition
-- Scanning 12.82MB of data
SELECT DISTINCT(PULocationID)
FROM ny_taxi.green_tripdata_non_partitoned
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-12-31';

-- Scanning 7.28 MB of DATA
SELECT DISTINCT(PULocationID)
FROM ny_taxi.green_tripdata_partitoned
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-12-31';

-- Creating a partition and cluster table
CREATE OR REPLACE TABLE ny_taxi.green_tripdata_partitoned_clustered
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY PUlocationID AS
SELECT * FROM `ny_taxi.greentaxi_tripdata`;

-- Query scans 7.28MB
SELECT count(*) as trips
FROM ny_taxi.green_tripdata_partitoned
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-12-31'
  AND PUlocationID=17;

-- Query scans 7.28MB
SELECT count(*) as trips
FROM ny_taxi.green_tripdata_partitoned_clustered
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-12-31'
  AND PUlocationID=17;
