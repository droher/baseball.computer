{% macro init_db(sample_factor=1, seed=0) %}
  {% set base_url = "https://baseball.computer" %}

    {% for node in graph.sources.values() %}
      {% set prefix = node.schema if node.schema in ("misc", "baseballdatabank") else "event" %}
      {% set sql %}
      CREATE SCHEMA IF NOT EXISTS {{ node.schema }};
      SET SCHEMA = '{{ node.schema }}';
      CREATE OR REPLACE TABLE {{ node.schema }}.{{ node.name }} AS (
        SELECT * FROM '{{ base_url }}/{{ prefix }}/{{ node.identifier }}.parquet'
        {% if node.schema == "event" and sample_factor > 1 %}
          WHERE HASH(event_key // 255) % {{ sample_factor }} = {{ seed }}
        {% endif %}
      );
      {% endset %}
      {% do log(sql, info=True)%}
      {% do run_query(sql) %}
    {% endfor %}

{% endmacro %}

{% macro create_enums() %}
  {% set sql %}
    DROP TYPE IF EXISTS base;
    DROP TYPE IF EXISTS baserunner;
    DROP TYPE IF EXISTS frame;
    DROP TYPE IF EXISTS side;
    DROP TYPE IF EXISTS hand;
    DROP TYPE IF EXISTS game_type;
    DROP TYPE IF EXISTS account_type;
    DROP TYPE IF EXISTS doubleheader_status;
    DROP TYPE IF EXISTS time_of_day;
    DROP TYPE IF EXISTS sky;
    DROP TYPE IF EXISTS field_condition;
    DROP TYPE IF EXISTS precipitation;
    DROP TYPE IF EXISTS wind_direction;
    DROP TYPE IF EXISTS plate_appearance_result;
    DROP TYPE IF EXISTS pitch_sequence_item;
    DROP TYPE IF EXISTS park_id;
    DROP TYPE IF EXISTS team_id;
    DROP TYPE IF EXISTS game_id;
    DROP TYPE IF EXISTS player_id;
    DROP TYPE IF EXISTS trajectory;
    DROP TYPE IF EXISTS location_general;
    DROP TYPE IF EXISTS location_depth;
    DROP TYPE IF EXISTS location_angle;
    DROP TYPE IF EXISTS baserunning_play;
    DROP TYPE IF EXISTS fielding_play;
    
  
    CREATE TYPE base AS ENUM ('Home', 'First', 'Second', 'Third');
    CREATE TYPE baserunner AS ENUM ('Batter', 'First', 'Second', 'Third');
    CREATE TYPE frame AS ENUM ('Top', 'Bottom');
    CREATE TYPE side AS ENUM ('Home', 'Away');
    -- TODO: Standardize
    CREATE TYPE hand AS ENUM ('L', 'R', 'B', '?', 'Left', 'Right');

    CREATE TYPE game_type AS ENUM (SELECT DISTINCT game_type FROM game.games ORDER BY 1);
    CREATE TYPE doubleheader_status AS ENUM (SELECT DISTINCT doubleheader_status FROM game.games ORDER BY 1);
    CREATE TYPE time_of_day AS ENUM (SELECT DISTINCT time_of_day FROM game.games ORDER BY 1);
    CREATE TYPE sky AS ENUM (SELECT DISTINCT sky FROM game.games ORDER BY 1);
    CREATE TYPE field_condition AS ENUM (SELECT DISTINCT field_condition FROM game.games ORDER BY 1);
    CREATE TYPE precipitation AS ENUM (SELECT DISTINCT precipitation FROM game.games ORDER BY 1);
    CREATE TYPE wind_direction AS ENUM (SELECT DISTINCT wind_direction FROM game.games ORDER BY 1);
    CREATE TYPE plate_appearance_result AS ENUM (SELECT DISTINCT plate_appearance_result FROM event.events WHERE plate_appearance_result IS NOT NULL ORDER BY 1);
    CREATE TYPE pitch_sequence_item AS ENUM (SELECT DISTINCT sequence_item FROM event.event_pitch_sequences ORDER BY 1);
    CREATE TYPE trajectory AS ENUM (SELECT DISTINCT batted_trajectory FROM event.events WHERE batted_trajectory IS NOT NULL ORDER BY 1);
    CREATE TYPE location_general AS ENUM (SELECT DISTINCT batted_location_general FROM event.events WHERE batted_location_general IS NOT NULL ORDER BY 1);
    CREATE TYPE location_depth AS ENUM (SELECT DISTINCT batted_location_depth FROM event.events WHERE batted_location_depth IS NOT NULL ORDER BY 1);
    CREATE TYPE location_angle AS ENUM (SELECT DISTINCT batted_location_angle FROM event.events WHERE batted_location_angle IS NOT NULL ORDER BY 1);
    CREATE TYPE baserunning_play AS ENUM (SELECT DISTINCT baserunning_play_type FROM event.event_baserunners WHERE baserunning_play_type IS NOT NULL ORDER BY 1);
    CREATE TYPE fielding_play AS ENUM (SELECT DISTINCT fielding_play FROM event.event_fielding_play ORDER BY 1);
    
    CREATE TYPE account_type AS ENUM (
      SELECT DISTINCT account_type FROM game.games
      UNION
      SELECT DISTINCT account_type FROM box_score.box_score_games
    );

    CREATE TYPE park_id AS ENUM (
      SELECT DISTINCT park_id FROM misc.park
      UNION
      -- TODO: Add missing NLB parks
      SELECT DISTINCT park_id FROM box_score.box_score_games WHERE park_id IS NOT NULL
      UNION
      SELECT DISTINCT park_id FROM game.games WHERE park_id IS NOT NULL
    );
    
    CREATE TYPE team_id AS ENUM (
      SELECT DISTINCT team_id FROM misc.roster
      UNION
      SELECT DISTINCT visiting_team FROM misc.gamelog
      UNION
      SELECT DISTINCT home_team FROM misc.gamelog
      UNION
      SELECT DISTINCT away_team_id FROM box_score.box_score_games
      UNION
      SELECT DISTINCT home_team_id FROM box_score.box_score_games
    );
    
    {# CREATE TYPE player_id AS ENUM (
      SELECT retro_id FROM baseballdatabank.people WHERE retro_id IS NOT NULL
      UNION
      SELECT DISTINCT player_id FROM misc.roster
      UNION
      SELECT DISTINCT batter_id FROM box_score.box_score_batting_lines
      UNION
      SELECT DISTINCT fielder_id FROM box_score.box_score_fielding_lines
    ); #}
    CREATE TYPE player_id AS VARCHAR;

    {# CREATE TYPE game_id AS ENUM (
      SELECT game_id FROM game.games
      UNION
      SELECT game_id FROM box_score.box_score_games
      UNION
      SELECT home_team || STRFTIME(date, '%Y%m%d') || double_header FROM misc.gamelog
    ); #}
    CREATE TYPE game_id AS VARCHAR;

  {% endset %}
  {% do log(sql, info=True)%}
  {% do run_query(sql) %}
{% endmacro %}

{% macro alter_types() %}
    {% for node in graph.sources.values() -%}
        {% set sql %}
      {% for col_name, col_data in node.columns.items() if col_data.get("data_type") -%}
          ALTER TABLE {{ node.schema }}.{{ node.name }} ALTER COLUMN "{{ col_name }}" TYPE {{ col_data.data_type }};
      {% endfor %}
        {% endset %}
        {% do log(sql, info=True)%}
        {% do run_query(sql) %}
    {% endfor %}
{% endmacro %}