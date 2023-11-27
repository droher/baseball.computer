WITH p AS (
        SELECT
            player_id,
            year_id AS year,
            ANY_VALUE(team_id) AS team_id,
            SUM(ipouts::int) / 3 AS ip,
            SUM(war::float) AS player_war,
            SUM(waa::float) AS player_waa,
            ROW_NUMBER() OVER (PARTITION BY year_id ORDER BY player_war DESC) AS war_rank,
            DENSE_RANK() OVER (PARTITION BY year ORDER BY ANY_VALUE(team_id)) AS team_num_id
        FROM main.war_pitch
        WHERE lg_id IN ('NL', 'AL', 'FL', 'UA', 'AA', 'NA', 'PL')
        GROUP BY 1, 2
    )

SELECT
    year,
    SUM(war_rank * ip) / SUM(ip) AS mean_inning_war_rank,
    MAX(team_num_id) AS num_teams,
    mean_inning_war_rank / num_teams AS mean_inning_war_rank_per_team
FROM p
GROUP BY 1 ORDER BY 1