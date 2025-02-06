-- models/staging/stg_orders.sql

with

source_2023 as (
    select * from {{ source('ecom', 'raw_orders_2023') }}
),

source_2024 as (
    select * from {{ source('ecom', 'raw_orders_2024') }}
),

combined_sources as (
    select * from source_2023
    union all
    select * from source_2024
),

renamed as (

    select

        ----------  ids
        id as order_id,
        store_id as location_id,
        customer as customer_id,

        ---------- numerics
        subtotal as subtotal_cents,
        tax_paid as tax_paid_cents,
        order_total as order_total_cents,
        -- Substituição manual do cents_to_dollars
        {{ cents_to_dollars('subtotal') }} as subtotal,
        -- Substituição manual do cents_to_dollars
        {{ cents_to_dollars('tax_paid') }} as tax_paid,
        -- Substituição manual do cents_to_dollars
        {{ cents_to_dollars('order_total') }} as order_total,

        ---------- timestamps
        -- Substituição manual do dbt.date_trunc
        date_trunc(ordered_at, day) as ordered_at

    from combined_sources

)

select * from renamed
