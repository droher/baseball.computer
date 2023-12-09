
    
    

with child as (
    select event_key as from_field
    from "timeball"."main_models"."stg_event_comments"
    where event_key is not null
),

parent as (
    select event_key as to_field
    from "timeball"."main_models"."stg_events"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


