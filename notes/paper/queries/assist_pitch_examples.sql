-- (a) assist_count_distribution: illustrative cells
-- Cell 1: bases empty, 0 out, routine groundout (documented example in docs/estimated-models.md)
-- Cell 2: runner on first, 0 out, ball in play out -- classic double-play-eligible state
SELECT result_family, base_state_start, outs_start, assist_count_class, ROUND(prob_mean, 3) AS prob_mean
FROM main_models.assist_count_distribution
WHERE (result_family = 'out_in_play' AND base_state_start = 0 AND outs_start = 0)
   OR (result_family = 'out_in_play' AND base_state_start = 1 AND outs_start = 0)
ORDER BY base_state_start, assist_count_class;

-- (b) pitch_summary_distribution: strikeouts, 2015 NL (documented example)
SELECT final_count_class, balls, strikes, ROUND(prob_mean, 3) AS prob_mean
FROM main_models.pitch_summary_distribution
WHERE result_family = 'strikeout' AND season = 2015 AND league = 'NL'
ORDER BY prob_mean DESC;
