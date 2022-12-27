with source as (
      select * from {{ source('game', 'game_team') }}
),
renamed as (
    select
        game_id,
        team_id,
        side

    from source
)
select * from renamed
  