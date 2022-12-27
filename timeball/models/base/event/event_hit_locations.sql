with source as (
      select * from {{ source('event', 'event_hit_location') }}
),
renamed as (
    select
        game_id,
        event_id,
        general_location,
        depth,
        angle,
        strength

    from source
)
select * from renamed
  