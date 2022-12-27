with source as (
      select * from {{ source('event', 'event_baserunning_advance_attempt') }}
),
renamed as (
    select
        game_id,
        event_id,
        sequence_id,
        baserunner,
        attempted_advance_to,
        is_successful,
        advanced_on_error_flag,
        rbi_flag,
        team_unearned_flag

    from source
)
select * from renamed
  