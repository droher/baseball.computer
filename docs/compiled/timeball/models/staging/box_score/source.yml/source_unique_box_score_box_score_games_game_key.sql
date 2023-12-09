
    
    

select
    game_key as unique_field,
    count(*) as n_records

from "timeball"."box_score"."box_score_games"
where game_key is not null
group by game_key
having count(*) > 1


