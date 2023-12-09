







with validation as (
  select
    
    sum(case when expected_batting_win_change is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion
  from "timeball"."main_models"."event_transition_values"
  
),
validation_errors as (
  select
    
    not_null_proportion
  from validation
  where not_null_proportion < 0.999 or not_null_proportion > 1
)
select
  *
from validation_errors

