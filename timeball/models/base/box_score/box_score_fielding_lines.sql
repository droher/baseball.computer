with source as (
      select * from {{ source('box_score', 'box_score_fielding_lines') }}
),
renamed as (
    select
        game_id,
        fielder_id,
        side,
        fielding_position,
        nth_position_played_by_player,
        outs_played,
        putouts,
        assists,
        errors,
        double_plays,
        triple_plays,
        passed_balls

    from source
)
select * from renamed
  