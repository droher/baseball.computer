MODEL (
  name main_models.stg_ejections,
  kind FULL,
  description 'Game ejections. From biodata/ejections.csv.',
  column_descriptions (
    game_id = @doc('game_id'),
    ejectee_id = 'Personnel id ejected.',
    ejectee_role = 'P=player, M=manager, C=coach, etc.',
    inning = 'Inning of ejection (-1 if pre-game).'
  ),
);






WITH source AS (
    SELECT * FROM biodata.ejections
),

renamed AS (
    SELECT
        game_id,
        date AS game_date,
        double_header AS doubleheader_status,
        ejectee AS ejectee_id,
        ejectee_name,
        team AS team_id,
        job AS ejectee_role,
        umpire AS umpire_id,
        umpire_name,
        inning,
        reason

    FROM source
)

SELECT * FROM renamed
