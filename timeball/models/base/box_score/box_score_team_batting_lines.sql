with source as (
      select * from {{ source('box_score', 'box_score_team_batting_lines') }}
),
renamed as (
    select
        game_id,
        side,
        at_bats,
        runs,
        hits,
        doubles,
        triples,
        home_runs,
        rbi,
        sacrifice_hits,
        sacrifice_flies,
        hit_by_pitch,
        walks,
        intentional_walks,
        strikeouts,
        stolen_bases,
        caught_stealing,
        grounded_into_double_plays,
        reached_on_interference

    from source
)
select * from renamed
  