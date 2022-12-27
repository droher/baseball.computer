with source as (
      select * from {{ source('event', 'event') }}
),
renamed as (
    select
        game_id,
        event_id,
        batting_side,
        inning,
        frame,
        at_bat,
        outs,
        count_balls,
        count_strikes

    from source
)
select * from renamed
  