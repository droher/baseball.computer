
    
    

with child as (
    select player_id as from_field
    from "timeball"."main_models"."stg_game_lineup_appearances"
    where player_id is not null
),

parent as (
    select player_id as to_field
    from "timeball"."main_models"."stg_bio"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


