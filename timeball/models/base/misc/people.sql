with source as (
      select * from {{ source('misc', 'people') }}
),
renamed as (
    select
        playerID,
        birthYear,
        birthMonth,
        birthDay,
        birthCountry,
        birthState,
        birthCity,
        deathYear,
        deathMonth,
        deathDay,
        deathCountry,
        deathState,
        deathCity,
        nameFirst,
        nameLast,
        nameGiven,
        weight,
        height,
        bats,
        throws,
        debut,
        finalGame,
        retroID,
        bbrefID

    from source
)
select * from renamed
  