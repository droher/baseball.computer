with source as (
      select * from {{ source('event', 'event_baserunning_play') }}
),
renamed as (
    select
        game_id,
        event_id,
        sequence_id,
        baserunning_play_type,
        at_base

    from source
)
select * from renamed
  