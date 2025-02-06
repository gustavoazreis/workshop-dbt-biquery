WITH

source AS (

    SELECT * FROM {{ source('ecom', 'raw_supplies') }}

),

renamed AS (

    SELECT

        ----------  ids
        -- Substituição manual do generate_surrogate_key
        id AS supply_id,
        sku AS product_id,
        name AS supply_name,

        ---------- text
        perishable AS is_perishable_supply,

        ---------- numerics
        -- Substituição manual do cents_to_dollars
        CONCAT(id, '-', sku) AS supply_uuid,

        ---------- booleans
        {{ cents_to_dollars('cost') }} AS supply_cost

    FROM source

)

SELECT * FROM renamed
