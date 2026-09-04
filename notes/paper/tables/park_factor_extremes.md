Note: `main_models.park_factor_summary` (`pf-full-ar1-v4`, non-centered gap-aware AR(1)) has only one `outcome` value, `team_runs`, so the outcome filter in the query below is a no-op kept for documentation clarity.

## (a) Top 8 and bottom 8 park-seasons by park factor

Coors Field (DEN02) dominates the top of the distribution across eight consecutive NL seasons (1995–2002), peaking at pf≈1.392 in 1996, while the bottom eight are split between CLE07 (Cleveland Municipal Stadium, 1939–1943 AL) and LOS03 (Dodger Stadium, 1963–1965 NL) at pf≈0.835–0.847.

| rank_group | park_id | season | league | park_factor_mean | park_factor_hdi_lower | park_factor_hdi_upper |
|------------|---------|-------:|--------|------------------:|-----------------------:|-----------------------:|
| top    | DEN02 | 1996 | NL | 1.392 | 1.323 | 1.461 |
| top    | DEN02 | 1995 | NL | 1.391 | 1.311 | 1.472 |
| top    | DEN02 | 1997 | NL | 1.383 | 1.320 | 1.446 |
| top    | DEN02 | 1998 | NL | 1.379 | 1.321 | 1.445 |
| top    | DEN02 | 1999 | NL | 1.374 | 1.316 | 1.437 |
| top    | DEN02 | 2000 | NL | 1.347 | 1.291 | 1.410 |
| top    | DEN02 | 2001 | NL | 1.323 | 1.267 | 1.383 |
| top    | DEN02 | 2002 | NL | 1.300 | 1.247 | 1.357 |
| bottom | LOS03 | 1963 | NL | 0.847 | 0.804 | 0.893 |
| bottom | LOS03 | 1964 | NL | 0.847 | 0.805 | 0.888 |
| bottom | CLE07 | 1943 | AL | 0.846 | 0.805 | 0.889 |
| bottom | CLE07 | 1939 | AL | 0.845 | 0.801 | 0.894 |
| bottom | LOS03 | 1965 | NL | 0.841 | 0.800 | 0.881 |
| bottom | CLE07 | 1941 | AL | 0.839 | 0.796 | 0.883 |
| bottom | CLE07 | 1942 | AL | 0.838 | 0.797 | 0.881 |
| bottom | CLE07 | 1940 | AL | 0.835 | 0.793 | 0.883 |

## (b) Average HDI width by league (park factor scale)

Sparse Negro-league seasons (NAL, NN2) and the short-lived Federal League (FL) carry 59–71% wider posterior HDIs than AL/NL, consistent with far fewer park-seasons to pool information across.

| league | n_park_seasons | avg_hdi_width |
|--------|----------------:|---------------:|
| NAL | 12   | 0.1590 |
| FL  | 16   | 0.1483 |
| NN2 | 19   | 0.1482 |
| NL  | 1288 | 0.0931 |
| AL  | 1301 | 0.0922 |

Note: the refit's persistence posterior is rho 0.956 (sd 0.008) with innovation sd 0.019; the group-level diagnostics for `rho_park` and `sigma_park_innov` are bulk ESS 3,261 and 1,891 with r-hat at most 1.0003, so the fit carries no weak-identification flag. Coors in 2015 reads 1.265 on this runs-per-plate-appearance definition.
