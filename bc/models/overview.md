{% docs __dbt_utils__ %}
Used for internal development only.
{% enddocs %}

{% docs __codegen__ %}
Used for internal development only.
{% enddocs %}

{% docs __overview__ %}
# baseball.computer

Welcome to the baseball.computer documentation site. The database uses a tool called `dbt` to build
out all of its tables. `dbt` also allows us to write documentation and tests for each table as part of its configuration.

This documentation is still a work in progress, but the majority of columns (around 75%) are documented. While the database
is still in preview, expect breaking changes to table/column names. As the project matures, these changes will be
minimized and spelled out clearly with each release.

## Getting started
The subset of tables below are a good places to get a feel for the database as well as some actually useful data.
dbt's individual table pages, in addition to column documentation, also contain the SQL used to generate the table
as well as the set of upstream and downstream dependencies.
### Metrics tables
The metrics tables are currently the most useful tables in the database for getting aggregate player information.
There are currently 9 of these tables:

#### Player career
- [metrics_player_career_offense]('https://docs.baseball.computer/#!/model/model.baseball_computer.metrics_player_career_offense')
- [metrics_player_career_pitching]('https://docs.baseball.computer/#!/model/model.baseball_computer.metrics_player_career_pitching')
- [metrics_player_career_fielding]('https://docs.baseball.computer/#!/model/model.baseball_computer.metrics_player_career_fielding')
#### Player season (separated by league)
- [metrics_player_season_league_offense]('https://docs.baseball.computer/#!/model/model.baseball_computer.metrics_player_season_league_offense')
- [metrics_player_season_league_pitching]('https://docs.baseball.computer/#!/model/model.baseball_computer.metrics_player_season_league_pitching')
- [metrics_player_season_league_fielding]('https://docs.baseball.computer/#!/model/model.baseball_computer.metrics_player_season_league_fielding')
#### Team season
- [metrics_team_season_offense]('https://docs.baseball.computer/#!/model/model.baseball_computer.metrics_team_season_offense')
- [metrics_team_season_pitching]('https://docs.baseball.computer/#!/model/model.baseball_computer.metrics_team_season_pitching')
- [metrics_team_season_fielding]('https://docs.baseball.computer/#!/model/model.baseball_computer.metrics_team_season_fielding')

### Event-level tables
The following tables will be the most useful for doing your own aggregations of event-level data:
- [event_offense_stats](https://docs.baseball.computer/#!/model/model.baseball_computer.event_offense_stats)
- [event_pitching_stats](https://docs.baseball.computer/#!/model/model.baseball_computer.event_pitching_stats)
- [event_player_fielding_stats](https://docs.baseball.computer/#!/model/model.baseball_computer.event_player_fielding_stats)
- [event_fielding_stats](https://docs.baseball.computer/#!/model/model.baseball_computer.event_fielding_stats)
- [event_states_full](https://docs.baseball.computer/#!/model/model.baseball_computer.event_states_full)

### Full history game-level tables
These tables include data for every game in MLB history, not just the ones for which we have event-level or box-level data.
- [game_start_info](https://docs.baseball.computer/#!/model/model.baseball_computer.game_start_info)
- [team_game_start_info](https://docs.baseball.computer/#!/model/model.baseball_computer.team_game_start_info)
- [game_results](https://docs.baseball.computer/#!/model/model.baseball_computer.game_results)
- [team_game_results](https://docs.baseball.computer/#!/model/model.baseball_computer.team_game_results)

### Other interesting/useful tables:
- [park_factors](https://docs.baseball.computer/#!/model/model.baseball_computer.park_factors) contains park factors calculated 
using a batter-pitcher-matched-pair methodology (and a more standard aggregate methodology as a fallback for years with insufficient data).

- [event_transition_values](https://docs.baseball.computer/#!/model/model.baseball_computer.event_transition_values) contains
the changes in run and win expectancy for each event in the database, which is calculated by several intermediate tables.

- [unknown_fielding_play_shares](https://docs.baseball.computer/#!/model/model.baseball_computer.unknown_fielding_play_shares) provides
a basic heuristic estimate of how much weight to assign to each fielder on plays when the fielder is unknown.

- [calc_batted_ball_type](https://docs.baseball.computer/#!/model/model.baseball_computer.calc_batted_ball_type) is an
example of a table that specifically focuses on applying complex logic to the event-level data to make it more useful downstream.

### Staging and seed tables
Moving all the way to the beginning of the pipeline, tables with the `main_models.stg_` prefix are staging tables. There is generally a 1:1 relationship between any staging table and an input
file to the database. The majority of tables in the database are built from a program that parses [Retrosheet](https://www.retrosheet.org/) event
data into a set of flat files. These tables will have the prefixes `stg_event` (for play-level data), `stg_game` (for game-level data),
and `stg_box_score` (for data derived from Retrosheet's box-score files). There are also a handful of miscellaneous files that are
based directly on flat CSV files that Retrosheet provides: rosters, schedules, park data, etc.

An important additional source for data is the [Baseball Databank](http://seanlahman.com/download-baseball-database/). This data,
also released under a Creative Commons license, is specifically used in our case to provide season-level player data on the years between 1873 and 1900,
which Retrosheet does not yet have. It also fills in some small gaps in Retrosheet's biographical information. These tables will have the prefix `stg_databank`.

Seed tables, located in the `main_seeds` schema, are tables constructed out of small CSV files that live directly in the repository.
These files are used to build out taxonomies and additional metadata for important concepts like batted ball locations and outcome result types.

### Analyses
The analyses folder contains sets of SQL queries that are either one-off calculations or not yet ready to be incorporated into the main
database. These are generally more experimental and less well-documented than the rest of the database, but feel free to poke around.

{% enddocs %}

