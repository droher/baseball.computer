with source as (
      select * from {{ source('misc', 'park') }}
),
renamed as (
    select
        park_id,
        name,
        aka,
        city,
        state,
        start_date,
        end_date,
        league,
        notes

    from source
)
select * from renamed
  