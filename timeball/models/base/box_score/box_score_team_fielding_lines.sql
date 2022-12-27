with source as (
      select * from {{ source('box_score', 'box_score_team_fielding_lines') }}
),
renamed as (
    select
        game_id,
        side,
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
  