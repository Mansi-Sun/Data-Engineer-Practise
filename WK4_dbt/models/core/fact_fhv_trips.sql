{{config(materialized='table')}}

with fhv as (
    select * from {{ref('stage_fhv_tripdata')}}
),
dim_zones as(
    select * from {{ref('dim_zones')}}
    where borough != 'Unknown'
)
select 
fhv.dispatching_base_num,
fhv.pickup_time,
fhv.dropoff_time,
pickup_zone.borough as pickup_borough,
dropoff_zone.borough as dropoff_borough,
fhv.Affiliated_base_number
from fhv
inner join dim_zones as pickup_zone
on fhv.pulocationid=pickup_zone.locationid
inner join dim_zones as dropoff_zone
on fhv.dolocationid=dropoff_zone.locationid