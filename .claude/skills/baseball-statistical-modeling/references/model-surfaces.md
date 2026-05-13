# Model Surfaces

Use this reference to map a modeling request to existing heuristics and candidate model families.

## Existing Heuristic Surfaces

| Surface | Current role | Modeling gap |
| --- | --- | --- |
| `event_completeness_*`, `game_data_completeness`, `player_game_data_completeness` | Boolean coverage flags. | No explicit observation model or uncertainty. |
| `calc_batted_ball_type` | Deterministic contact and location inference. | Strong evidence is mixed with latent geometry and scorer/source bias. |
| `player_position_game_fielding_stats` | Event versus box authority order. | No posterior allocation for unknown event credits or no-box gaps. |
| `unknown_fielding_play_shares` | Residual allocation using box surplus and unassisted-putout rates. | Point heuristic, limited context, no posterior uncertainty. |
| `scorekeeper_tendencies_*` | Grouped scorer/decade rates. | No partial pooling or label-confusion model. |
| `ground_ball_blame` | Window ratios from selected high-coverage eras. | Selection bias and shift-era transport risk. |
| `runner_advance_expectancy`, `fielder_advance_expectancy` | Cell averages with residual adjustment. | Sparse cells and unstable conditioning. |
| `linear_weights` | Exact cells above threshold, generic fallback below. | Hard thresholds and point imputation. |
| `park_factors` | Batter/pitcher self-join with fixed synthetic prior sample size. | Fixed pseudo-counts are not uncertainty estimates. |
| `ml_features`, `predictions_*` | Predictive artifacts. | Calibration, leakage, and publication boundary must be explicit. |

## Build Order

1. Source acquisition and issue reliability ledgers.
2. Identity, personnel, context, and exposure reliability ledgers.
3. Event observation ledger.
4. Fielding credit gap classification.
5. Scorer/source observation models for batted-ball detail.
6. Box-anchored fielding credit allocation.
7. Broad handler and geometry probabilities.
8. Scorer-normalized detailed contact labels.
9. Fielding responsibility and advancement.
10. Observation-adjusted park factors.
11. Run values and context-adjusted metrics.
12. Pitch sequence and synthetic event/personnel generation only after upstream assumptions validate.

## Candidate Families

### Observation And Missingness

Use hierarchical Bernoulli, ordinal, or multinomial models for whether a dimension is observed, unknown-coded, source-block missing, or structurally absent. Pool by era, league, source family, scorer, team, park, game type, and event salience only when connectivity supports separation.

### Contact Label Confusion

Use label-confusion models when recorded trajectory or location is a scorer/source measurement of latent contact rather than truth. Keep broad ground/air classes ahead of detailed line/fly/pop distinctions.

### Fielding Credit Allocation

Use constrained categorical, multinomial, or Poisson allocation models with eligible personnel masks and aggregate box constraints. Separate official credit from physical responsibility.

### Handler And Geometry

Use hierarchical categorical models over broad fielder, side, depth, and region. Treat deterministic batted-ball rules as high-confidence measurements with failure modes, not as universal truth.

### Park Factors

Use hierarchical park-season-league models with player, pitcher, team, opponent, schedule, handedness, weather, and observation-bias controls. Report posterior uncertainty and weak-identification flags.

### Run Values

Use hierarchical run expectancy or linear-weight models with season, league, state, and event-type pooling. Preserve official scoring and event reconstruction channels separately.

### Advancement And Responsibility

Use ordinal or categorical models over runner advancement and fielder responsibility only after geometry and personnel reliability are validated. Avoid conditioning on post-treatment outcome labels when estimating opportunity.

### ML And Deep Learning Supplements

Use ML or deep models for calibrated proposals, embeddings, residual discovery, or sequence baselines. Do not publish argmax predictions as facts.
