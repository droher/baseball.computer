WITH source AS (
    SELECT * FROM "timeball"."misc"."bio"
),

renamed AS (
    SELECT
        player_id,
        last AS last_name,
        first AS official_name,
        nickname AS first_name,
        birthdate AS birth_date,
        birth_city,
        birth_state,
        birth_country,
        play_debut AS player_debut_date,
        play_lastgame AS player_last_game_date,
        mgr_debut AS manager_debut_date,
        mgr_lastgame AS manager_last_game_date,
        coach_debut AS coach_debut_date,
        coach_lastgame AS coach_last_game_date,
        ump_debut AS umpire_debut_date,
        ump_lastgame AS umpire_last_game_date,
        deathdate AS death_date,
        death_city AS death_city,
        death_state AS death_state,
        death_country AS death_country,
        bats,
        throws,
        CASE WHEN height LIKE '%-%'
            THEN SPLIT_PART(height, '-', 1)::INT * 12 + SPLIT_PART(height, '-', 2)::INT
        END AS height_inches,
        weight AS weight_pounds,
        -- TODO: Fix spelling
        cemetary AS cemetery_name,
        ceme_city AS cemetery_city,
        ceme_state AS cemetery_state,
        ceme_country AS cemetery_country,
        ceme_note AS cemetery_note,
        birth_name AS birth_name,
        name_chg AS name_change_notes,
        bat_chg AS batting_hand_change_notes,
        hof AS hall_of_fame_status

    FROM source
)

SELECT * FROM renamed