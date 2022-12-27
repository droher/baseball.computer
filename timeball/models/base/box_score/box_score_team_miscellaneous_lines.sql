with source as (
      select * from {{ source('box_score', 'box_score_team_miscellaneous_lines') }}
),
renamed as (
    select
        game_id,
        side,
        left_on_base,
        team_earned_runs,
        double_plays_turned,
        triple_plays_turned

    from source
)
select * from renamed
  