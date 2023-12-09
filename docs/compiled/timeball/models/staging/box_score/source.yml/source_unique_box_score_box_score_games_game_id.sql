
    
    

select
    game_id as unique_field,
    count(*) as n_records

from "timeball"."box_score"."box_score_games"
where game_id is not null
group by game_id
having count(*) > 1


