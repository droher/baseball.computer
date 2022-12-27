with source as (
      select * from {{ source('misc', 'roster') }}
),
renamed as (
    select
        year,
        player_id,
        last_name,
        first_name,
        bats,
        throws,
        team_id,
        position

    from source
)
select * from renamed
  