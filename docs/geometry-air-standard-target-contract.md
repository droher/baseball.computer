# Airborne standardization target contract

Contract: `trajectory-air-standard-v1`. This supersedes the earlier proposal to assign all contact to launch-angle bands. The user clarified: **preserve ground versus air; standardize airborne balls**. The change precedes standardized model fitting and acquisition of the 121 deferred modern-angle evaluation games.

Ground balls stay GroundBall regardless of launch angle, including bunts. For airborne contact, apply LineDrive at 10 through 25 degrees, Fly above 25 through 50, and PopUp above 50. Bunt remains a separate attribute. Preserve original labels, raw angle, and the unconditional angle-band diagnostic as separate fields.

For modern reference construction, derive broad type separately from the local recorded subtype and Statcast category. If both agree, retain their broad type. If only one is available, retain it with its source explicitly identified. If both exist and disagree, mark the reference unresolved and preserve both source values; do not change a historical ground ball into an airborne class or vice versa to improve agreement. A modern conflict is withheld from target calibration and remains in the population accounting.

An airborne observation with angle below 10 degrees remains airborne but has unresolved standardized subtype (`air_angle_conflict`). A missing airborne angle is `air_angle_missing`. Neither becomes GroundBall. Unknown broad type stays unresolved even if an angle is present. Unverified game/event matching supplies no calibration target. Invalid numerical angles fail validation. Ground balls do not require a launch-angle measurement for this definition.

Calibration consumers require a non-null trajectory with `ground_preserved` or `air_angle_standardized` status. A non-null broad observation alone is insufficient: unresolved pairing can retain source observations while supplying no calibration target.

At historical prediction time, a known ground-ball observation constrains standardized probability to GroundBall; a known airborne observation constrains predictions to the three airborne categories. Missing broad type requires a probabilistic estimate with its own validation. Preserving an observed ground/air distinction is not evidence of imputation accuracy; evaluation must separately test reconstruction with that distinction unavailable.

Public Statcast values may be estimates, and broad source labels are observations rather than instrument-certified facts. The modern reference is therefore an explicit operational standard with uncertainty and provenance limitations. No current artifact establishes historical transportability or reliability of naturally missing outcomes.

The ongoing `20260911-statcast-fitting-v1` acquisition was started under the previous angle-band proposal. Its `angle_standardized_class` column is retained strictly as the unconditional diagnostic computed by that archived collector. The canonical target will be constructed separately under this contract; raw downloads and game selection remain useful and unchanged.
