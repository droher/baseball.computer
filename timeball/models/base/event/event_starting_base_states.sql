with source as (
      select * from {{ source('event', 'event_starting_base_state') }}
),
renamed as (
    select
        game_id,
        event_id,
        baserunner,
        runner_lineup_position,
        charged_to_pitcher_id

    from source
)
select * from renamed
  