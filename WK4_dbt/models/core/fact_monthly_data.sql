{{
    config(
        materialized='table'
    )
}}

with green_tripdata as (
    select *, 
        'Green' as service_type
    from {{ ref('stage_green_tripdata') }}
), 
yellow_tripdata as (
    select *, 
        'Yellow' as service_type
    from {{ ref('stage_yellow_tripdata') }}
), 
trips_unioned as (
    select * from green_tripdata
    union all 
    select * from yellow_tripdata
), 
dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
),
fhv as (
    select *,
    'fhv' as service_type
    from {{ref('stage_fhv_tripdata')}}
)
(select 
    trips_unioned.service_type,
    trips_unioned.pickup_time, 
    trips_unioned.dropoff_time, 
from trips_unioned
inner join dim_zones as pickup_zone
on trips_unioned.pulocationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on trips_unioned.dolocationid = dropoff_zone.locationid)
union all
(select 
fhv.service_type,
fhv.pickup_time,
fhv.dropoff_time
from fhv)