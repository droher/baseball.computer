with source as (
      select * from {{ source('event', 'event_fielding_play') }}
),
renamed as (
    select
        game_id,
        event_id,
        sequence_id,
        fielding_position,
        fielding_play

    from source
)
select * from renamed
  