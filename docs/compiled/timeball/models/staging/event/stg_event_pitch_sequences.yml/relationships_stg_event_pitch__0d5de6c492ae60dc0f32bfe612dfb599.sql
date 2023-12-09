
    
    

with child as (
    select game_id as from_field
    from "timeball"."main_models"."stg_event_pitch_sequences"
    where game_id is not null
),

parent as (
    select game_id as to_field
    from "timeball"."main_models"."stg_games"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


