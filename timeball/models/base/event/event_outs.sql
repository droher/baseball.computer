with source as (
      select * from {{ source('event', 'event_out') }}
),
renamed as (
    select
        game_id,
        event_id,
        sequence_id,
        baserunner_out

    from source
)
select * from renamed
  