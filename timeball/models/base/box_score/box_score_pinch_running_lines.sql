with source as (
      select * from {{ source('box_score', 'box_score_pinch_running_lines') }}
),
renamed as (
    select
        game_id,
        pinch_runner_id,
        inning,
        side,
        runs,
        stolen_bases,
        caught_stealing

    from source
)
select * from renamed
  