WITH

source AS (

    SELECT * FROM {{ source('ecom', 'raw_stores') }}

),

renamed AS (

    SELECT

        ----------  ids
        id AS location_id,

        ---------- text
        name AS location_name,

        ---------- numerics
        tax_rate,

        ---------- timestamps
        -- Substituição manual do dbt.date_trunc
        DATE_TRUNC(opened_at, DAY) AS opened_date

    FROM source

)

SELECT * FROM renamed
