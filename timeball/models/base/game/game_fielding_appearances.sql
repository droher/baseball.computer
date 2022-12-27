with source as (
      select * from {{ source('game', 'game_fielding_appearance') }}
),
renamed as (
    select
        game_id,
        player_id,
        side,
        fielding_position,
        start_event_id,
        end_event_id

    from source
)
select * from renamed
  