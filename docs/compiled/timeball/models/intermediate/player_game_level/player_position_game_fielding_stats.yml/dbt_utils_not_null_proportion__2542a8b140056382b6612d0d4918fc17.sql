







with validation as (
  select
    
    sum(case when double_plays is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion
  from "timeball"."main_models"."player_position_game_fielding_stats"
  
),
validation_errors as (
  select
    
    not_null_proportion
  from validation
  where not_null_proportion < 0.995 or not_null_proportion > 1
)
select
  *
from validation_errors

