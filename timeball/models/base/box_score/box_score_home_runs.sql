with source as (
      select * from {{ source('box_score', 'box_score_home_runs') }}
),
renamed as (
    select
        game_id,
        batting_side,
        batter_id,
        pitcher_id,
        inning,
        runners_on,
        outs

    from source
)
select * from renamed
  