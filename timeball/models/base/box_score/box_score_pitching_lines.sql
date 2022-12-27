with source as (
      select * from {{ source('box_score', 'box_score_pitching_lines') }}
),
renamed as (
    select
        game_id,
        pitcher_id,
        side,
        nth_pitcher,
        outs_recorded,
        no_out_batters,
        batters_faced,
        hits,
        doubles,
        triples,
        home_runs,
        runs,
        earned_runs,
        walks,
        intentional_walks,
        strikeouts,
        hit_batsmen,
        wild_pitches,
        balks,
        sacrifice_hits,
        sacrifice_flies

    from source
)
select * from renamed
  