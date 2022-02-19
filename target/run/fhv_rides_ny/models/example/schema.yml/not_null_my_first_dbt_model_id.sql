select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select *
from `zoom-camp-project-23525`.`dbt_mpremi`.`my_first_dbt_model`
where id is null



      
    ) dbt_internal_test