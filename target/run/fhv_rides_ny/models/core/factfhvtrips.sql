

  create or replace table `zoom-camp-project-23525`.`dbt_mpremi`.`factfhvtrips`
  
  
  OPTIONS()
  as (
    

with fhv_data as (
    select * from `zoom-camp-project-23525`.`dbt_mpremi`.`stg_fhv_tripdata`
), 

dim_zones as (
    select * from `zoom-camp-project-23525`.`dbt_mpremi`.`dim_zones`
    where borough != 'Unknown'
)

select
    fhv_data.tripid, 
    fhv_data.dispatching_base_num, 
    fhv_data.pickup_locationid, 
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    fhv_data.dropoff_locationid,
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone,  
    fhv_data.pickup_datetime, 
    fhv_data.dropoff_datetime, 
    fhv_data.sr_flag, 
    fhv_data.a_base_number
from fhv_data
inner join dim_zones as pickup_zone
on fhv_data.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on fhv_data.dropoff_locationid = dropoff_zone.locationid
  );
  