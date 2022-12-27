with source as (
      select * from {{ source('box_score', 'box_score_team') }}
),
renamed as (
    select
        game_id,
        team_id,
        side

    from source
)
select * from renamed
  