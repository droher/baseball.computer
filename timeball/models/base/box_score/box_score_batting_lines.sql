with source as (
      select * from {{ source('box_score', 'box_score_batting_lines') }}
),
renamed as (
    select
        game_id,
        batter_id,
        side,
        lineup_position,
        nth_player_at_position,
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
  