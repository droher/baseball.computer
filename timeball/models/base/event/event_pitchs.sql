with source as (
      select * from {{ source('event', 'event_pitch') }}
),
renamed as (
    select
        game_id,
        event_id,
        sequence_id,
        sequence_item,
        runners_going_flag,
        blocked_by_catcher_flag,
        catcher_pickoff_attempt_at_base

    from source
)
select * from renamed
  