
    
    

select
    retro_id as unique_field,
    count(*) as n_records

from "timeball"."baseballdatabank"."people"
where retro_id is not null
group by retro_id
having count(*) > 1


