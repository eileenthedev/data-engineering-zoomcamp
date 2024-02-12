# Data Engineering Zoomcamp HW3 Answers

1. UPLOAD Green-Taxi-Parquet Files in GCS
2. In Big Query, create a new dataset and external table using the GCS links
3. Then, in Big query:

SETUP: CREATE MATERIALIZED TABLE
```
CREATE OR REPLACE TABLE `smiling-breaker-413716.taxi_rides_ny.green_tripdata_nonpartitioned`
AS SELECT * FROM `smiling-breaker-413716.taxi_rides_ny.green_tripdata
```

QUESTION 1: 840,402
```
SELECT COUNT(*) FROM `smiling-breaker-413716.taxi_rides_ny.green_tripdata`
```

QUESTION 2: 0 MB for the External Table and 6.41MB for the Materialized Table
```
SELECT COUNT(DISTINCT PULocationID) FROM `smiling-breaker-413716.taxi_rides_ny.green_tripdata`
SELECT COUNT(DISTINCT PULocationID) FROM `smiling-breaker-413716.taxi_rides_ny.green_tripdata_nonpartitioned`
```

QUESTION 3: 1,622
```
SELECT COUNT(*) FROM `smiling-breaker-413716.taxi_rides_ny.green_tripdata` WHERE fare_amount = 0
```

QUESTION 4:  Partition by lpep_pickup_datetime Cluster on PUlocationID
```
CREATE OR REPLACE TABLE `smiling-breaker-413716.taxi_rides_ny.green_tripdata_partitioned`
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY PUlocationID AS (
  SELECT * FROM `smiling-breaker-413716.taxi_rides_ny.green_tripdata`
);
```

QUESTION 5: 12.82 MB for non-partitioned table and 1.12 MB for the partitioned table
```
SELECT COUNT(DISTINCT PULocationID) FROM  `smiling-breaker-413716.taxi_rides_ny.green_tripdata_nonpartitioned`
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30'

SELECT COUNT(DISTINCT PULocationID) FROM  `smiling-breaker-413716.taxi_rides_ny.green_tripdata_partitioned`
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30'
```

QUESTION 8:
```
SELECT * FROM `smiling-breaker-413716.taxi_rides_ny.green_tripdata_partitioned`
```
It will read all lines and columns not using the power of partitions because we didn't define the columns in SELECT statement
