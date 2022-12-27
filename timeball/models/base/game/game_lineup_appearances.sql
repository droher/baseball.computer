with source as (
      select * from {{ source('game', 'game_lineup_appearance') }}
),
renamed as (
    select
        game_id,
        player_id,
        side,
        lineup_position,
        entered_game_as,
        start_event_id,
        end_event_id

    from source
)
select * from renamed
  