with source as (
      select * from {{ source('misc', 'schedule') }}
),
renamed as (
    select
        date,
        double_header,
        day_of_week,
        visiting_team,
        visiting_team_league,
        visiting_team_game_number,
        home_team,
        home_team_league,
        home_team_game_number,
        day_night,
        postponement_indicator,
        makeup_dates

    from source
)
select * from renamed
  