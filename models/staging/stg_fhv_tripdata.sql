
{{ config(materialized='view') }}


select 
    {{ dbt_utils.surrogate_key(['dispatching_base_num', 'Affiliated_base_number', 'pickup_datetime']) }} as tripid,
    dispatching_base_num,
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime  as timestamp) as dropoff_datetime,
    cast(PULocationID as integer) pickup_locationid,
    cast(DOLocationID as integer) dropoff_locationid,
    SR_Flag as sr_flag,
    Affiliated_base_number as a_base_number,

from {{ source('staging', 'external_for_hire_vehicles_tripdata') }}
limit 100