## Week 3 Data Warehouse

-Data Warehouse and BigQuery

### Learning Notes

-BigQuery partitioining VS clustering

-Internals of BigQuery

### Homework scripts

--Create an external table,materilized table 

create or replace external table ny_taxi.green_tripdata_2022
options(
  format = 'PARQUET',
  uris = ['gs://mage-demo-mansisun007654321/green_tripdata_2022*.parquet']
);

create or replace table ny_taxi.green_tripdata_2022_new
as select * from `ny_taxi.green_tripdata_2022`;

--Question 1: What is count of records for the 2022 Green Taxi Data??

select count(*) from ny_taxi.green_tripdata_2022;

--Question 2:Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

select distinct PULocationID from ny_taxi.green_tripdata_2022;

select distinct PULocationID from ny_taxi.green_tripdata_2022_new;

--Question 3:How many records have a fare_amount of 0?

select count(*) from ny_taxi.green_tripdata_2022
where fare_amount=0;

--Question 4 What is the best strategy to make an optimized table in Big Query if your query will always order the results by PUlocationID and filter based on lpep_pickup_datetime? (Create a new table with this strategy)

create or replace table ny_taxi.green_tripdata_2022_partitioned
partition by date(lpep_pickup_datetime)
cluster by PULocationID
as select * from `ny_taxi.green_tripdata_2022_new`;

--Question 5 Write a query to retrieve the distinct PULocationID between lpep_pickup_datetime 06/01/2022 and 06/30/2022 (inclusive) Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values?

select distinct(PULocationID)
from `ny_taxi.green_tripdata_2022_partitioned`
where date(lpep_pickup_datetime) between '2022-06-01' and '2022-06-30';

--Question 8:Write a SELECT count(*) query FROM the materialized table you created. How many bytes does it estimate will be read? Why?

select count(*) from `ny_taxi.green_tripdata_2022_new`
