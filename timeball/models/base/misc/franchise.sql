with source as (
      select * from {{ source('misc', 'franchise') }}
),
renamed as (
    select
        current_franchise_id,
        team_id,
        league,
        division,
        location,
        nickname,
        alternate_nicknames,
        date_start,
        date_end,
        city,
        state

    from source
)
select * from renamed
  