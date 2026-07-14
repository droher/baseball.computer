Note: `main_models.park_factor_summary` has only one `outcome` value, `team_runs`, so the outcome filter in the query below is a no-op kept for documentation clarity.

## (a) Top 8 and bottom 8 park-seasons by park factor

Coors Field (DEN02) dominates the top of the distribution across eight consecutive NL seasons (1995–2002), peaking at pf≈1.392 in 1996, while the bottom is split between LOS03 (Dodger Stadium, early-1960s NL) and CLE07 (Cleveland Municipal Stadium, early-1940s AL) around pf≈0.84–0.85.

| rank_group | park_id | season | league | park_factor_mean | park_factor_hdi_lower | park_factor_hdi_upper |
|------------|---------|-------:|--------|------------------:|-----------------------:|-----------------------:|
| top    | DEN02 | 1996 | NL | 1.392 | 1.321 | 1.461 |
| top    | DEN02 | 1995 | NL | 1.391 | 1.31  | 1.47  |
| top    | DEN02 | 1997 | NL | 1.383 | 1.321 | 1.45  |
| top    | DEN02 | 1998 | NL | 1.378 | 1.319 | 1.442 |
| top    | DEN02 | 1999 | NL | 1.373 | 1.313 | 1.433 |
| top    | DEN02 | 2000 | NL | 1.346 | 1.289 | 1.404 |
| top    | DEN02 | 2001 | NL | 1.322 | 1.267 | 1.38  |
| top    | DEN02 | 2002 | NL | 1.299 | 1.244 | 1.356 |
| bottom | LOS03 | 1962 | NL | 0.852 | 0.805 | 0.901 |
| bottom | CLE07 | 1943 | AL | 0.85  | 0.808 | 0.891 |
| bottom | LOS03 | 1963 | NL | 0.847 | 0.804 | 0.891 |
| bottom | LOS03 | 1964 | NL | 0.847 | 0.806 | 0.888 |
| bottom | CLE07 | 1941 | AL | 0.844 | 0.803 | 0.888 |
| bottom | CLE07 | 1942 | AL | 0.843 | 0.803 | 0.888 |
| bottom | CLE07 | 1940 | AL | 0.842 | 0.8   | 0.888 |
| bottom | LOS03 | 1965 | NL | 0.841 | 0.801 | 0.88  |

## (b) Average HDI width by league (park factor scale)

Sparse Negro-league seasons (NAL, NN2) and the short-lived Federal League (FL) carry 60–75% wider posterior HDIs than AL/NL, consistent with far fewer park-seasons to pool information across.

| league | n_park_seasons | avg_hdi_width |
|--------|----------------:|---------------:|
| NAL | 12   | 0.1582 |
| FL  | 16   | 0.1472 |
| NN2 | 19   | 0.1445 |
| NL  | 1288 | 0.0917 |
| AL  | 1301 | 0.0909 |
