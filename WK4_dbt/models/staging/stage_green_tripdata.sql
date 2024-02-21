{{config(materialized='view')}}

select 
--identifiers
{{dbt_utils.surrogate_key(['VendorID','lpep_pickup_datetime'])}} as tripid,
cast(VendorID as integer) as vendorid,
cast(ratecodeid as integer) as ratecodeid,
cast(PULocationID as integer) as pulocationid,
cast(DOLocationID as integer) as dolocationid,
--timestamps
cast(lpep_pickup_datetime as timestamp) as pickup_time,
cast(lpep_dropoff_datetime as timestamp) as dropoff_time,
--trip info
store_and_fwd_flag, 
cast(trip_distance as numeric) as trip_distance,
cast(trip_type as integer) as trip_type,
cast(passenger_count as integer) as passenger_count,
--payment info
cast(fare_amount as numeric) as fare_amount,
cast(extra as numeric) as extra,
cast(mta_tax as numeric) as mta_tax,
cast(tip_amount as numeric) as tip_amount,
cast(tolls_amount as numeric) as tolls_amount,
cast(0 as numeric) as ehail_fee,
cast(improvement_surcharge as numeric) as improvement_surcharge,
cast(total_amount as numeric) as total_amount,
cast(payment_type as integer) as payment_type,
{{get_payment_type_description('payment_type')}} as payment_type_description,
cast(congestion_surcharge as numeric) as congestion_surcharge

from {{source('staging','green_tripdata_external')}}
where VendorID is not null
{% if var('is_test_run',default=true) %}
limit 100
{% endif %}