with source as (
      select * from {{ source('box_score', 'box_score_stolen_bases') }}
),
renamed as (
    select
        game_id,
        running_side,
        runner_id,
        pitcher_id,
        catcher_id,
        inning

    from source
)
select * from renamed
  