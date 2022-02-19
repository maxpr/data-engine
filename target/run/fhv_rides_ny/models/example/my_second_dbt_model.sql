

  create or replace view `zoom-camp-project-23525`.`dbt_mpremi`.`my_second_dbt_model`
  OPTIONS()
  as -- Use the `ref` function to select from other models

select *
from `zoom-camp-project-23525`.`dbt_mpremi`.`my_first_dbt_model`
where id = 1;

