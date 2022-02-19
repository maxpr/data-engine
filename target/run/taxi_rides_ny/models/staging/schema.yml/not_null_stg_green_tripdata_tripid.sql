select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select *
from `zoom-camp-project-23525`.`dbt_mpremi`.`stg_green_tripdata`
where tripid is null



      
    ) dbt_internal_test