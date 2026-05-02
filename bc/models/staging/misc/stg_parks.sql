MODEL (
  name main_models.stg_parks,
  kind FULL,
  grain (park_id),
  columns (
    park_id PARK_ID,
    name VARCHAR,
    aka VARCHAR,
    city VARCHAR,
    state VARCHAR,
    start_date VARCHAR,
    end_date VARCHAR,
    league VARCHAR,
    notes VARCHAR
  ),
  column_descriptions (
    park_id = @doc('park_id'),
    league = @doc('league')
  ),
  physical_properties (
    download_parquet = 'https://data.baseball.computer/dbt/main_models_stg_parks.parquet'
  ),
);







WITH source AS (
    SELECT * FROM misc.park
),

renamed AS (
    SELECT
        park_id::park_id AS park_id,
        name,
        aka,
        city,
        state,
        start_date,
        end_date,
        league,
        notes

    FROM source
)

SELECT * FROM renamed
