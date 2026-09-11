MODEL (
  name main_models.event_observation_context,
  kind FULL,
  description 'Shared modeling-dataset feeder. Wide event-grain (one row per event_key) table denormalizing covariates so every model_input_* dataset INNER JOINs this rather than re-deriving the same joins. source_type/source_family/target_population_status pull from source_acquisition_ledger filtered to dimension=event, team_id IS NULL. park_episode_status is NULL in v1 pending park-renovation enrichment of entity_link_reliability. scorer is the legacy ambiguous compatibility field. official_scorer and source_scorer preserve raw info,oscorer and info,scorer values separately; their statuses come from game_context_observation_ledger. inputter/translator come direct from stg_games. affiliated_team rolls legacy game_scorekeeping up to one row per game_id via MAX(game_share), then projects scorer_more_common_team_id. score_margin is event_states_full.batting_team_margin_start. leverage_index is win_leverage_index from leverage_index (joined on win_expectancy_start_key). hit_or_out is BOOLEAN: TRUE = batted-ball hit, FALSE = batted-ball out, NULL = non-batted-ball (walk/HBP/K/no-PA). Derived from event_offense_stats (baserunner=Batter): balls_batted=1 AND hits=1 -> TRUE; balls_batted=1 AND hits=0 -> FALSE; else NULL. personnel_confidence rolls personnel_state_reliability per event_key: low if any reliability_class IN (synthetic, ambiguous); medium if any inferred; else high. v1 collapses to high/medium because the v1 upstream emits only direct + inferred. context_confidence preserves the pre-provenance rollup across the original 23 atomic dimensions: low if any observed_status=missing; medium if any observed_status=unknown_code; else high. The two new scorer dimensions are excluded until consumers deliberately adopt them. Deterministic derived statuses (bio-derived hands, rule_era flags) are high-confidence and do not demote the rollup. exposure_status is game_exposure_ledger.completion_status joined on (game_id, batting_team_id).',
  grain (event_key),
  columns (
    event_key UINTEGER,
    game_id VARCHAR,
    season SMALLINT,
    league VARCHAR,
    game_type GAME_TYPE,
    source_type VARCHAR,
    source_family VARCHAR,
    target_population_status VARCHAR,
    park_id PARK_ID,
    park_episode_status VARCHAR,
    scorer VARCHAR,
    official_scorer VARCHAR,
    official_scorer_status VARCHAR,
    source_scorer VARCHAR,
    source_scorer_status VARCHAR,
    inputter VARCHAR,
    translator VARCHAR,
    affiliated_team TEAM_ID,
    inning_start UTINYINT,
    frame_start FRAME,
    base_state_start UTINYINT,
    outs_start UTINYINT,
    score_margin TINYINT,
    leverage_index DOUBLE,
    runs_on_play UTINYINT,
    hit_or_out BOOLEAN,
    batter_id VARCHAR,
    pitcher_id VARCHAR,
    batter_hand HAND,
    pitcher_hand HAND,
    batting_team_id TEAM_ID,
    fielding_team_id TEAM_ID,
    personnel_confidence VARCHAR,
    context_confidence VARCHAR,
    exposure_status VARCHAR,
    result_family VARCHAR,
    alignment_regime VARCHAR,
    leverage_bucket VARCHAR
  ),
  column_descriptions (
    event_key = @doc('event_key'),
    game_id = @doc('game_id'),
    season = @doc('season'),
    league = @doc('league'),
    game_type = @doc('game_type'),
    source_type = 'source_acquisition_ledger.source_type for (game_id, dimension=event, team_id IS NULL). PlayByPlay, BoxScore, GameLog, or NULL when no source covers the game.',
    source_family = 'source_acquisition_ledger.source_family for the same key. Coarsest source family: play_by_play, box_score, gamelog, derived, absent.',
    target_population_status = 'source_acquisition_ledger.target_population_status: event_level, aggregate_only, gamelog_only, structural_absence, coverage_within_source_sparse, out_of_scope.',
    park_id = @doc('park_id'),
    park_episode_status = 'Park-episode classification (e.g., pre/post renovation). NULL in v1 — waits on park-renovation enrichment of entity_link_reliability.',
    scorer = 'Legacy ambiguous stg_games.scorer compatibility value. It may originate from info,oscorer or info,scorer depending on raw record order and must not be treated as a certified official-scorer identity.',
    official_scorer = 'Raw stg_games.official_scorer value from info,oscorer only. NULL when absent or blank; never filled from legacy scorer or source_scorer.',
    official_scorer_status = 'Observation status for official_scorer from game_context_observation_ledger: observed, unknown_code, or missing.',
    source_scorer = 'Raw stg_games.source_scorer administrative value from info,scorer only. NULL when absent or blank; not an official-scorer identity.',
    source_scorer_status = 'Observation status for source_scorer from game_context_observation_ledger: observed, unknown_code, or missing.',
    inputter = 'stg_games.inputter (raw).',
    translator = 'stg_games.translator (raw).',
    affiliated_team = 'game_scorekeeping.scorer_more_common_team_id of the dominant-game_share scorer per game. May be NULL when no scorer or no team affiliation is known.',
    inning_start = @doc('inning_start'),
    frame_start = @doc('frame_start'),
    base_state_start = @doc('base_state_start'),
    outs_start = @doc('outs_start'),
    score_margin = 'event_states_full.batting_team_margin_start. Batting team score minus fielding team score at event start.',
    leverage_index = 'leverage_index.win_leverage_index keyed on event_states_full.win_expectancy_start_key. NULL when no key match.',
    runs_on_play = @doc('runs_on_play'),
    hit_or_out = 'BOOLEAN. TRUE = batted-ball event resulting in a hit; FALSE = batted-ball event resulting in an out; NULL otherwise (walks, HBP, strikeouts, non-PA). Derived from event_offense_stats (baserunner=Batter): balls_batted=1 AND hits=1 -> TRUE; balls_batted=1 AND hits=0 -> FALSE; else NULL.',
    batter_id = @doc('batter_id'),
    pitcher_id = @doc('pitcher_id'),
    batter_hand = 'Resolved batter handedness at event time (event_states_full.batter_hand).',
    pitcher_hand = 'Resolved pitcher handedness at event time (event_states_full.pitcher_hand).',
    batting_team_id = 'Batting team for the event (event_states_full.batting_team_id).',
    fielding_team_id = 'Fielding team for the event (event_states_full.fielding_team_id).',
    personnel_confidence = 'Rollup of personnel_state_reliability per event_key. low if any reliability_class IN (synthetic, ambiguous); medium if any inferred; else high. v1 reachable: high (full direct coverage) or medium (any missing slot).',
    context_confidence = 'Rollup of game_context_observation_ledger per game_id across 23 atomic context dimensions. low if any observed_status=missing; medium if any observed_status=unknown_code; else high (all observed/derived/not_applicable). Deterministic derived statuses stay high-confidence.',
    exposure_status = 'game_exposure_ledger.completion_status joined on (game_id, batting_team_id). complete, walk_off, shortened, suspended, forfeit, unknown.',
    result_family = 'Coarse PA result family. hit / out_in_play / strikeout / walk / hbp / sacrifice / reached_on_error / fielders_choice / interference. NULL for no-play and baserunning-only events.',
    alignment_regime = 'Categorical season-era fallback for the shift-propensity model. pre_shift_era <=2009, shift_growth_era 2010-2014, full_shift_era 2015-2022, post_restriction >=2023.',
    leverage_bucket = 'Leverage bucket from win_leverage_index. low <0.8, medium 0.8-<2.0, high >=2.0. NULL when leverage_index is NULL.'
  ),
  audits (
    not_null(columns := (event_key, game_id, season, game_type, batting_team_id, fielding_team_id, inning_start, frame_start, outs_start, base_state_start, exposure_status, source_family, source_type, target_population_status, official_scorer_status, source_scorer_status)),
    unique_grain(columns := (event_key)),
    accepted_values(column := source_family, is_in := ('play_by_play', 'box_score', 'gamelog', 'derived', 'absent')),
    accepted_values(column := target_population_status, is_in := ('event_level', 'aggregate_only', 'gamelog_only', 'structural_absence', 'out_of_scope', 'coverage_within_source_sparse')),
    accepted_values(column := exposure_status, is_in := ('complete', 'walk_off', 'shortened', 'suspended', 'forfeit', 'unknown')),
    accepted_values(column := personnel_confidence, is_in := ('high', 'medium', 'low')),
    accepted_values(column := context_confidence, is_in := ('high', 'medium', 'low')),
    accepted_values(column := official_scorer_status, is_in := ('observed', 'unknown_code', 'missing')),
    accepted_values(column := source_scorer_status, is_in := ('observed', 'unknown_code', 'missing')),
    accepted_values(column := result_family, is_in := (
      'hit', 'out_in_play', 'strikeout', 'walk', 'hbp',
      'sacrifice', 'reached_on_error', 'fielders_choice', 'interference'
    )),
    accepted_values(column := alignment_regime, is_in := (
      'pre_shift_era', 'shift_growth_era', 'full_shift_era', 'post_restriction'
    )),
    accepted_values(column := leverage_bucket, is_in := ('low', 'medium', 'high'))
  )
);

WITH evt AS (
    SELECT
        event_key,
        game_id,
        season,
        league,
        game_type,
        park_id,
        inning_start,
        frame_start,
        base_state_start,
        outs_start,
        batting_team_margin_start,
        runs_on_play,
        batter_id,
        pitcher_id,
        batter_hand,
        pitcher_hand,
        batting_team_id,
        fielding_team_id,
        win_expectancy_start_key
    FROM main_models.event_states_full
    WHERE season BETWEEN @start_season AND @end_season
),

evp AS (
    SELECT
        event_key,
        plate_appearance_result
    FROM main_models.stg_events
),

gms AS (
    SELECT
        game_id,
        scorer,
        official_scorer,
        source_scorer,
        inputter,
        translator
    FROM main_models.stg_games
),

src AS (
    SELECT
        game_id,
        source_type,
        source_family,
        target_population_status
    FROM main_models.source_acquisition_ledger
    WHERE dimension = 'event' AND team_id IS NULL
),

sko AS (
    SELECT
        game_id,
        scorer_more_common_team_id AS affiliated_team
    FROM main_models.game_scorekeeping
    QUALIFY ROW_NUMBER() OVER (PARTITION BY game_id ORDER BY game_share DESC, cleaned_scorer) = 1
),

lev AS (
    SELECT
        win_expectancy_start_key,
        win_leverage_index
    FROM main_models.leverage_index
),

ofs AS (
    SELECT
        event_key,
        balls_batted,
        hits
    FROM main_models.event_offense_stats
    WHERE baserunner = 'Batter'
),

prs AS (
    SELECT
        event_key,
        CASE
            WHEN BOOL_OR(reliability_class IN ('synthetic', 'ambiguous')) THEN 'low'
            WHEN BOOL_OR(reliability_class = 'inferred') THEN 'medium'
            ELSE 'high'
        END AS personnel_confidence
    FROM main_models.personnel_state_reliability
    GROUP BY 1
),

gco AS (
    SELECT
        game_id,
        CASE
            WHEN BOOL_OR(observed_status = 'missing') THEN 'low'
            WHEN BOOL_OR(observed_status = 'unknown_code') THEN 'medium'
            ELSE 'high'
        END AS context_confidence
    FROM main_models.game_context_observation_ledger
    WHERE context_dimension NOT IN ('official_scorer', 'source_scorer')
    GROUP BY 1
),

gsc AS (
    SELECT
        game_id,
        MAX(observed_status) FILTER (
            WHERE context_dimension = 'official_scorer'
        ) AS official_scorer_status,
        MAX(observed_status) FILTER (
            WHERE context_dimension = 'source_scorer'
        ) AS source_scorer_status
    FROM main_models.game_context_observation_ledger
    WHERE context_dimension IN ('official_scorer', 'source_scorer')
    GROUP BY 1
),

gex AS (
    SELECT
        game_id,
        team_id,
        completion_status
    FROM main_models.game_exposure_ledger
)

SELECT
    evt.event_key,
    evt.game_id,
    evt.season,
    evt.league,
    evt.game_type,
    src.source_type,
    src.source_family,
    src.target_population_status,
    evt.park_id,
    CAST(NULL AS VARCHAR) AS park_episode_status,
    gms.scorer,
    gms.official_scorer,
    gsc.official_scorer_status,
    gms.source_scorer,
    gsc.source_scorer_status,
    gms.inputter,
    gms.translator,
    sko.affiliated_team,
    evt.inning_start,
    evt.frame_start,
    evt.base_state_start,
    evt.outs_start,
    evt.batting_team_margin_start AS score_margin,
    lev.win_leverage_index AS leverage_index,
    evt.runs_on_play,
    CASE
        WHEN ofs.balls_batted = 1 AND ofs.hits = 1 THEN TRUE
        WHEN ofs.balls_batted = 1 AND ofs.hits = 0 THEN FALSE
        ELSE NULL
    END AS hit_or_out,
    evt.batter_id,
    evt.pitcher_id,
    evt.batter_hand,
    evt.pitcher_hand,
    evt.batting_team_id,
    evt.fielding_team_id,
    prs.personnel_confidence,
    gco.context_confidence,
    gex.completion_status AS exposure_status,
    CASE
        WHEN evp.plate_appearance_result IN ('Single', 'Double', 'GroundRuleDouble', 'Triple', 'HomeRun', 'InsideTheParkHomeRun')
            THEN 'hit'
        WHEN evp.plate_appearance_result = 'InPlayOut' THEN 'out_in_play'
        WHEN evp.plate_appearance_result = 'StrikeOut' THEN 'strikeout'
        WHEN evp.plate_appearance_result IN ('Walk', 'IntentionalWalk') THEN 'walk'
        WHEN evp.plate_appearance_result = 'HitByPitch' THEN 'hbp'
        WHEN evp.plate_appearance_result IN ('SacrificeFly', 'SacrificeHit') THEN 'sacrifice'
        WHEN evp.plate_appearance_result = 'ReachedOnError' THEN 'reached_on_error'
        WHEN evp.plate_appearance_result = 'FieldersChoice' THEN 'fielders_choice'
        WHEN evp.plate_appearance_result = 'Interference' THEN 'interference'
        ELSE NULL
    END AS result_family,
    CASE
        WHEN evt.season <= 2009 THEN 'pre_shift_era'
        WHEN evt.season BETWEEN 2010 AND 2014 THEN 'shift_growth_era'
        WHEN evt.season BETWEEN 2015 AND 2022 THEN 'full_shift_era'
        WHEN evt.season >= 2023 THEN 'post_restriction'
    END AS alignment_regime,
    CASE
        WHEN lev.win_leverage_index IS NULL THEN NULL
        WHEN lev.win_leverage_index < 0.8 THEN 'low'
        WHEN lev.win_leverage_index < 2.0 THEN 'medium'
        ELSE 'high'
    END AS leverage_bucket
FROM evt
LEFT JOIN gms USING (game_id)
LEFT JOIN src USING (game_id)
LEFT JOIN sko USING (game_id)
LEFT JOIN lev ON lev.win_expectancy_start_key = evt.win_expectancy_start_key
LEFT JOIN ofs USING (event_key)
LEFT JOIN prs USING (event_key)
LEFT JOIN gco USING (game_id)
LEFT JOIN gsc USING (game_id)
LEFT JOIN gex ON gex.game_id = evt.game_id AND gex.team_id = evt.batting_team_id
LEFT JOIN evp USING (event_key)
