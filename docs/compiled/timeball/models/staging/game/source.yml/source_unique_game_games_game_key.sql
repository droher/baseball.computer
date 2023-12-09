
    
    

select
    game_key as unique_field,
    count(*) as n_records

from "timeball"."game"."games"
where game_key is not null
group by game_key
having count(*) > 1


