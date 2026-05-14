AUDIT (
  name eligible_for_allocation_requires_hard_mask
);

SELECT event_key, fielding_team_id, eligible_for_allocation, personnel_hard_mask_available
FROM @this_model
WHERE eligible_for_allocation AND NOT personnel_hard_mask_available
