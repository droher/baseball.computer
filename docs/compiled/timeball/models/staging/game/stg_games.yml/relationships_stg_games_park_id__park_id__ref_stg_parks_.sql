
    
    

with child as (
    select park_id as from_field
    from "timeball"."main_models"."stg_games"
    where park_id is not null
),

parent as (
    select park_id as to_field
    from "timeball"."main_models"."stg_parks"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


