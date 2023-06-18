CREATE TYPE BATTING_STAT_TYPE AS ENUM (
    SELECT name
    FROM {{ ref('seed_stat_categories') }}
    WHERE is_batting
);
CREATE TYPE PITCHING_STAT_TYPE AS ENUM (
    SELECT name
    FROM {{ ref('seed_stat_categories') }}
    WHERE is_pitching
);
CREATE TYPE FIELDING_STAT_TYPE AS ENUM (
    SELECT name
    FROM {{ ref('seed_stat_categories') }}
    WHERE is_fielding
);
-- TODO: use these after resolution of https://github.com/duckdb/duckdb/issues/7707
{# CREATE TYPE BATTING_STATS AS MAP(BATTING_STAT_TYPE, INT);
CREATE TYPE PITCHING_STATS AS MAP(PITCHING_STAT_TYPE, INT);
CREATE TYPE FIELDING_STATS AS MAP(FIELDING_STAT_TYPE, INT); #}
