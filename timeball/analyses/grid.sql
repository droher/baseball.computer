with
    teams as (select distinct team_id from stg_rosters where year = 2005),

    grid as (
        select t1.team_id as team1, t2.team_id as team2
        from teams t1
        join teams t2 on t1.team_id < t2.team_id
    ),


    player_teams as (
        select distinct player_id, team_id
        from stg_rosters
        where team_id in (select distinct team_id FROM stg_rosters where year = 2005)
    ),

    player_grid as (
        select t1.player_id, [t1.team_id, t2.team_id] as teams,
        from player_teams t1
        join player_teams t2
            on t1.player_id = t2.player_id
            and t1.team_id < t2.team_id
        --where t1.team_id IN (select distinct team_id from stg_rosters where year = 2005)
    ),

    roster_sizes as (
        select r.year, r.team_id, COUNT(*) as total_pairs, COUNT(DISTINCT teams) as distinct_pairs
        from stg_rosters r
        join player_grid using (player_id)
        group by 1, 2
    )


SELECT * FROM roster_sizes
ORDER BY 4 desc, 3 desc