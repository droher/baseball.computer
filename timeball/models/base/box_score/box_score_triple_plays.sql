with source as (
      select * from {{ source('box_score', 'box_score_triple_plays') }}
),
renamed as (
    select
        game_id,
        defense_side,
        fielders

    from source
)
select * from renamed
  