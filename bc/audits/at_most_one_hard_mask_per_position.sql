AUDIT (
  name at_most_one_hard_mask_per_position
);

WITH grouped AS (
  SELECT event_key, fielding_side, fielding_position, COUNT(*) AS n_hard_mask
  FROM @this_model
  WHERE hard_zero_allowed = TRUE
  GROUP BY event_key, fielding_side, fielding_position
)

SELECT g.event_key, g.fielding_side, g.fielding_position, g.n_hard_mask FROM grouped AS g
WHERE g.n_hard_mask > 1
