


select 
    locationid, 
    borough, 
    zone,
    replace(service_zone,'Boro','Green') as service_zone
from `zoom-camp-project-23525`.`dbt_mpremi`.`taxi_zone_lookup`