with source as (
      select * from {{ source('game', 'game_umpire') }}
),
renamed as (
    select
        game_id,
        position,
        umpire_id

    from source
)
select * from renamed
  