WITH source AS (
    SELECT * FROM {{ source('biodata', 'ejections') }}
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
