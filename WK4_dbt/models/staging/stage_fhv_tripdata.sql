{{config(materialized='view')}}

select
dispatching_base_num,
cast(pickup_datetime as timestamp) as pickup_time,
cast(dropOff_datetime as timestamp) as dropoff_time,
cast(PULocationID as integer) as pulocationid,
cast(DOLocationID as integer) as dolocationid,
sr_flag,
Affiliated_base_number
from {{source("staging","fhv_2019")}}
where extract(year from pickup_datetime)=2019
{% if var('is_test_run',default=true) %}
limit 100
{% endif %}
