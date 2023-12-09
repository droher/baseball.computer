
    
    

select
    playerid as unique_field,
    count(*) as n_records

from "timeball"."baseballdatabank"."people"
where playerid is not null
group by playerid
having count(*) > 1


