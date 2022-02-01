CREATE OR REPLACE EXTERNAL TABLE `zoom-camp-project-23525.trips_data_all.external_for_hire_vehicles_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['https://storage.cloud.google.com/dtc_data_lake_zoom-camp-project-23525/raw/for_hire_vehicle_2019-*.parquet', 'https://storage.cloud.google.com/dtc_data_lake_zoom-camp-project-23525/raw/for_hire_vehicle_2020-*.parquet', 'https://storage.cloud.google.com/dtc_data_lake_zoom-camp-project-23525/raw/for_hire_vehicle_2021-*.parquet']
);

CREATE OR REPLACE TABLE zoom-camp-project-23525.trips_data_all.external_for_hire_vehicles_tripdata_partitoned
PARTITION BY
  DATE(pickup_datetime) AS
SELECT * FROM zoom-camp-project-23525.trips_data_all.external_for_hire_vehicles_tripdata;


SELECT DISTINCT(dispatching_base_num)
FROM zoom-camp-project-23525.trips_data_all.external_for_hire_vehicles_tripdata_partitoned
WHERE DATE(pickup_datetime) BETWEEN '2019-06-01' AND '2019-06-30';

SELECT DISTINCT(dispatching_base_num)
FROM zoom-camp-project-23525.trips_data_all.external_for_hire_vehicles_tripdata
WHERE DATE(pickup_datetime) BETWEEN '2019-06-01' AND '2019-06-30';

SELECT COUNT(DISTINCT(dropoff_datetime))
FROM zoom-camp-project-23525.trips_data_all.external_for_hire_vehicles_tripdata_partitoned

SELECT COUNT(DISTINCT(dispatching_base_num))
FROM zoom-camp-project-23525.trips_data_all.external_for_hire_vehicles_tripdata_partitoned
-- 914

SELECT COUNT(DISTINCT(PULocationID))
FROM zoom-camp-project-23525.trips_data_all.external_for_hire_vehicles_tripdata_partitoned
-- 264

SELECT COUNT(DISTINCT(DOLocationID))
FROM zoom-camp-project-23525.trips_data_all.external_for_hire_vehicles_tripdata_partitoned
-- 265

SELECT COUNT(DISTINCT(SR_Flag))
FROM zoom-camp-project-23525.trips_data_all.external_for_hire_vehicles_tripdata_partitoned
--43

SELECT COUNT(DISTINCT(Affiliated_base_number))
FROM zoom-camp-project-23525.trips_data_all.external_for_hire_vehicles_tripdata_partitoned
-- 1629

-- Question 1:
-- What is count for fhv vehicles data for year 2019
SELECT COUNT(*) FROM zoom-camp-project-23525.trips_data_all.external_for_hire_vehicles_tripdata_partitoned
WHERE DATE(pickup_datetime) BETWEEN '2019-01-01' AND '2019-12-31';
-- 	42084899

-- Question 2:
-- How many distinct dispatching_base_num we have in fhv for 2019
-- 643 MB
SELECT COUNT(DISTINCT(dispatching_base_num)) FROM zoom-camp-project-23525.trips_data_all.external_for_hire_vehicles_tripdata_partitoned
WHERE DATE(pickup_datetime) BETWEEN '2019-01-01' AND '2019-12-31';
-- 	792

-- Question 3:
-- Best strategy to optimise if query always filter by dropoff_datetime and order by dispatching_base_num

-- CREATE OR REPLACE TABLE zoom-camp-project-23525.trips_data_all.external_for_hire_vehicles_tripdata_clustered_q3
-- PARTITION BY DATE(dropoff_datetime)
-- CLUSTER BY dispatching_base_num AS
-- SELECT * FROM zoom-camp-project-23525.trips_data_all.external_for_hire_vehicles_tripdata;

-- Question 4:
-- What is the count, estimated and actual data processed for query which counts trip between 2019/01/01 and 2019/03/31 for dispatching_base_num B00987, B02060, B02279
-- Create a table with optimized clustering and partitioning, and run a count(*). Estimated data processed can be found in top right corner and actual data processed can be found after the query is executed.
CREATE OR REPLACE TABLE zoom-camp-project-23525.trips_data_all.external_for_hire_vehicles_tripdata_clustered_q4
PARTITION BY DATE(pickup_datetime)
CLUSTER BY dispatching_base_num AS
SELECT * FROM zoom-camp-project-23525.trips_data_all.external_for_hire_vehicles_tripdata;

SELECT COUNT(*) FROM zoom-camp-project-23525.trips_data_all.external_for_hire_vehicles_tripdata
WHERE DATE(pickup_datetime) BETWEEN '2019-01-01' AND '2019-03-31' AND dispatching_base_num IN ('B00987','B02060','B02279');
-- Query complete (8.4 sec elapsed, 400.1 MB processed) 

SELECT COUNT(*) FROM zoom-camp-project-23525.trips_data_all.external_for_hire_vehicles_tripdata_partitoned
WHERE DATE(pickup_datetime) BETWEEN '2019-01-01' AND '2019-03-31' AND dispatching_base_num IN ('B00987','B02060','B02279');
-- Query complete (0.6 sec elapsed, 400.1 MB processed)

SELECT COUNT(*) FROM zoom-camp-project-23525.trips_data_all.external_for_hire_vehicles_tripdata_clustered_q4
WHERE DATE(pickup_datetime) BETWEEN '2019-01-01' AND '2019-03-31' AND dispatching_base_num IN ('B00987','B02060','B02279');
-- Query complete (0.4 sec elapsed, 156.3 MB processed)

-- Question 5:
-- What will be the best partitioning or clustering strategy when filtering on dispatching_base_num and SR_Flag
-- Review partitioning and clustering video. Clustering cannot be created on all data types.
CREATE OR REPLACE TABLE zoom-camp-project-23525.trips_data_all.external_for_hire_vehicles_tripdata_clustered_q5_1
CLUSTER BY dispatching_base_num, SR_Flag AS
SELECT * FROM zoom-camp-project-23525.trips_data_all.external_for_hire_vehicles_tripdata;

SELECT DISTINCT(SR_Flag) from zoom-camp-project-23525.trips_data_all.external_for_hire_vehicles_tripdata_clustered_q5_1;
-- Query complete (0.3 sec elapsed, 41.1 MB processed)

SELECT DISTINCT(SR_Flag) from zoom-camp-project-23525.trips_data_all.external_for_hire_vehicles_tripdata_partitoned;
-- Query complete (0.5 sec elapsed, 41.1 MB processed)



SELECT dispatching_base_num,SR_Flag,count(*) FROM zoom-camp-project-23525.trips_data_all.external_for_hire_vehicles_tripdata_clustered_q5_1
group by dispatching_base_num, SR_Flag;
-- Query complete (0.2 sec elapsed, 530.8 MB processed)

SELECT dispatching_base_num,SR_Flag,count(*) FROM zoom-camp-project-23525.trips_data_all.external_for_hire_vehicles_tripdata_partitoned
group by dispatching_base_num, SR_Flag;
-- Query complete (0.6 sec elapsed, 530.8 MB processed)

SELECT count(*) FROM zoom-camp-project-23525.trips_data_all.external_for_hire_vehicles_tripdata_partitoned
WHERE dispatching_base_num IN ('B02835','B02878','B02279', 'B02765', 'B02682') AND SR_Flag IN (11, 20, 10);
-- Query complete (0.5 sec elapsed, 530.8 MB processed)

SELECT count(*) FROM zoom-camp-project-23525.trips_data_all.external_for_hire_vehicles_tripdata_clustered_q5_1
WHERE dispatching_base_num IN ('B02835','B02878','B02279', 'B02765', 'B02682') AND SR_Flag IN (11, 20, 10);
-- Query complete (0.2 sec elapsed, 31.3 MB processed


SELECT COUNT(*) FROM zoom-camp-project-23525.trips_data_all.external_for_hire_vehicles_tripdata_clustered_q5_1
WHERE SR_Flag = 4 AND  dispatching_base_num IN ('B00987','B02060','B02279');
--Query complete (0.2 sec elapsed, 9.5 MB processed)


CREATE OR REPLACE TABLE zoom-camp-project-23525.trips_data_all.external_for_hire_vehicles_tripdata_clustered_q5_2
CLUSTER BY SR_Flag,dispatching_base_num AS
SELECT * FROM zoom-camp-project-23525.trips_data_all.external_for_hire_vehicles_tripdata;

SELECT count(*) FROM zoom-camp-project-23525.trips_data_all.external_for_hire_vehicles_tripdata_clustered_q5_2
WHERE dispatching_base_num IN ('B02835','B02878','B02279', 'B02765', 'B02682') AND SR_Flag IN (11, 20, 10);
--  Query complete (0.2 sec elapsed, 27.7 MB processed)

SELECT COUNT(*) FROM zoom-camp-project-23525.trips_data_all.external_for_hire_vehicles_tripdata_clustered_q5_2
WHERE SR_Flag = 4 AND  dispatching_base_num IN ('B00987','B02060','B02279');
-- Query complete (0.2 sec elapsed, 21.1 MB processed)




