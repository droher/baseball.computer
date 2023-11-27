WITH b AS (
        SELECT
            player_id,
            year_id AS year,
            FIRST(name_common) AS name,
            FIRST(year_id::int - age::int) AS birth_year,
            MIN(year_id // 10 * 10) AS debut_decade,
            SUM(waa::float) AS waa,
            SUM(war::float) AS war,
            FIRST(salary::int) AS salary,
            SUM(pa::int) AS pa
        FROM main.war_bat
        WHERE lg_id IN ('NL', 'AL', 'FL', 'UA', 'AA', 'NA', 'PL')
        GROUP BY 1, 2
        ORDER BY 2, 5 DESC
    ),
    p AS (
        SELECT
            player_id,
            year_id AS year,
            SUM(ipouts::int) AS ip_outs,
            SUM(war::float) AS war,
            SUM(waa::float) AS waa,
            FIRST(salary::int) AS salary,
        FROM main.war_pitch
        GROUP BY 1, 2
    ),
    war AS (
        SELECT
            b.player_id AS baseball_reference_player_id,
            b.year,
            b.debut_decade,
            b.name,
            b.birth_year,
            COALESCE(b.waa, 0) + COALESCE(p.waa, 0) AS _waa,
            COALESCE(b.war, 0) + COALESCE(p.war, 0) AS _war,
            COALESCE(b.salary, p.salary) AS salary,
            b.pa,
            p.ip_outs
        FROM b
        LEFT JOIN p USING (player_id, year)
    ),

t AS (
    SELECT player_id,
        ANY_VALUE(baseball_reference_player_id) AS baseball_reference_player_id,
        ANY_VALUE(COALESCE(r.region, c.region, 'Other/Unknown')) AS region,
        ANY_VALUE(birth_country) AS country,
        MIN(season // 10 * 10) AS debut_decade,
        MAX(season) - ANY_VALUE(birth_year) AS retirement_age,
        SUM(CASE WHEN fielding_position = 1 THEN outs_played ELSE outs_played / 3 END) AS outs_played
    FROM {{ ref('player_position_team_season_fielding_lines') }} f
    LEFT JOIN {{ ref('people') }} p USING (player_id)
    LEFT JOIN {{ ref('seed_us_states_regions') }} r ON r.state = p.birth_state
    LEFT JOIN {{ ref('seed_country_regions') }} c USING (birth_country)
    WHERE f.team_id IN (SELECT DISTINCT team_id FROM {{ ref('team_game_start_info') }} WHERE league IN ('AL', 'NL', 'FL', 'AA', 'PL', 'UA', 'NA'))
    GROUP BY 1
),

grouped AS (
SELECT
    COALESCE(region, 'International') AS region,
    year,
    COUNT(*) AS players,
    SUM(_war) AS war,
    SUM(_waa) AS waa,
    SUM(_war)/COUNT(*) AS war_per_player,
    SUM(GREATEST(_waa, 0)) AS positive_waa,
    -SUM(LEAST(_waa, 0)) AS negative_waa,
    COUNT_IF(_waa > 0) AS positive_players,
    COUNT_IF(_waa < 0) AS negative_players,
    positive_waa / SUM(positive_waa) OVER (PARTITION BY w.year) AS positive_waa_share,
    negative_waa / SUM(negative_waa) OVER (PARTITION BY w.year) AS negative_waa_share,
    positive_players / SUM(positive_players) OVER (PARTITION BY w.year) AS positive_player_share,
    negative_players / SUM(negative_players) OVER (PARTITION BY w.year) AS negative_player_share,
    players / SUM(players) OVER (PARTITION BY year) AS player_share,
    war / SUM(war) OVER (PARTITION BY year) AS war_share,
FROM war AS w
LEFT JOIN t USING (baseball_reference_player_id)
GROUP BY 1, 2
)

SELECT year,
    region,
    player_share,
    war_share,
    positive_waa_share,
    negative_waa_share,
    positive_waa_share - negative_waa_share AS waa_diff,
    positive_player_share,
    negative_player_share,
    positive_player_share - negative_player_share AS player_diff,
    AVG(1 / player_share) OVER w AS player_share_scale_factor,
    AVG(1 / positive_waa_share) OVER w AS positive_waa_share_scale_factor,
    AVG(1 / war_share) OVER w AS war_share_scale_factor,
FROM grouped
WHERE YEAR < 2023
WINDOW w AS (PARTITION BY region ORDER BY year RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING)
QUALIFY war_share > .01 OR player_share > .01
ORDER BY 1, 5 DESC
