with source as (
      select * from {{ source('box_score', 'box_score_hit_by_pitches') }}
),
renamed as (
    select
        game_id,
        pitching_side,
        pitcher_id,
        batter_id

    from source
)
select * from renamed
  