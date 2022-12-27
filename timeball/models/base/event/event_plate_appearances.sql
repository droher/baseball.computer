with source as (
      select * from {{ source('event', 'event_plate_appearance') }}
),
renamed as (
    select
        game_id,
        event_id,
        plate_appearance_result,
        contact,
        hit_to_fielder

    from source
)
select * from renamed
  