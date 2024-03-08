## Risingwave workshop learning notes

![Dashboard](./img/dashboard.gif)

Check out this well prepared workshop resouce from:

```
git clone git@github.com:risingwavelabs/risingwave-data-talks-workshop-2024-03-04.git
```

Tutorial available from https://www.youtube.com/watch?v=L2BHFnZ6XjE


### Key takeaway 

- Hands on experience with streaming processing

- Sharpen your SQL skills (dynamic filter,window function)

- Peeping a little bit database internal

### Following the workshop step by step

> Note: If you wanna run this on your computer,make sure you have enough disk space (>15GB), and with postgresql & docker installed on your mac.

#### 1.Setup the environment
```
# Check version of psql
psql --version
source commands.sh

# Start the RW cluster
start-cluster

# Setup python
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```
> Note: Why using source here, instead of "./commands.sh"? The reason is to make the environment variables effect on current session. If we run "./commands.sh", it will create a new session and run the scripts then exit the new session. So if we run "echo $PGHOST", will get a blank result.

#### 2.Simulate the real-time data
```
stream-kafka
```
> Note: We could find below code from "commands.sh"
```
# Seed trip data from the parquet file
stream-kafka() {
        ./seed_kafka.py update
}
```

The script will convert the date in "data/yellow_tripdata_2022-01.parquet" file into today's date, and sending the data continually.

![real time data](./img/real-time.png)

Open another terminal to create the trip_data table:
```
source commands.sh
psql -f risingwave-sql/table/trip_data.sql
psql -c "show tables;"
```

#### 3.Validate the ingested data
![verify data](./img/verifty%20data.png)

#### 4.Create materialized view for later on display

##### MV1 Top 10 busiest zones in last 1 minutes
```
create materialized view last_1min_top10_busiest_zones as
select 
    zone as pickup_zone,
    count(*) as pickup_counts
from taxi_zone
    join trip_data
    on taxi_zone.location_id=trip_data.pulocationid
where
    trip_data.tpep_pickup_datetime > now() - interval '1 minute'
group by zone
order by pickup_counts desc
limit 10;
```

##### MV2 Longest trips in last 5 minutes
```
create materialized view last_5mins_longest_trips as
select 
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    pickupzone.zone as pickup_zone,
    dropoffzone.zone as dropoff_zone,
    trip_distance
from trip_data
    join
        taxi_zone as pickupzone on pickupzone.location_id=trip_data.pulocationid
    join
        taxi_zone as dropoffzone on dropoffzone.location_id=trip_data.dolocationid
where tpep_pickup_datetime > now()- interval '5 minute'
order by trip_distance desc
limit 10;
```
#### Checking the dashboard
If it's a new session, remember to run below commands

> Remember to change the materialized view name in server.py

```
source commands.sh
# Setup python
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Start Server
./server.py

open index.html
```
