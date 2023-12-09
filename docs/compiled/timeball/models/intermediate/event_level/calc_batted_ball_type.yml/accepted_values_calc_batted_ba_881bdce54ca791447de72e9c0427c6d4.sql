
    
    

with all_values as (

    select
        location_edge as value_field,
        count(*) as n_records

    from "timeball"."main_models"."calc_batted_ball_type"
    group by location_edge

)

select *
from all_values
where value_field not in (
    'Left','Middle','Right','Unknown'
)


