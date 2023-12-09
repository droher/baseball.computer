
    
    

select
    park_id as unique_field,
    count(*) as n_records

from "timeball"."misc"."park"
where park_id is not null
group by park_id
having count(*) > 1


