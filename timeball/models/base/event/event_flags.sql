with source as (
      select * from {{ source('event', 'event_flag') }}
),
renamed as (
    select
        game_id,
        event_id,
        sequence_id,
        flag

    from source
)
select * from renamed
  