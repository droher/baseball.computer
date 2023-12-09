
    
    

select
    bbrefid as unique_field,
    count(*) as n_records

from "timeball"."baseballdatabank"."people"
where bbrefid is not null
group by bbrefid
having count(*) > 1


