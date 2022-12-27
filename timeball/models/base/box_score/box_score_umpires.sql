with source as (
      select * from {{ source('box_score', 'box_score_umpire') }}
),
renamed as (
    select
        game_id,
        position,
        umpire_id

    from source
)
select * from renamed
  