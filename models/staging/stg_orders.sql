WITH source AS (
    SELECT
        *
    FROM
        {{ ref('raw_orders') }}
),
renamed AS (
    SELECT
        ---------- ids
        id AS order_id,
        store_id AS location_id,
        customer AS customer_id,
        ---------- numerics
        subtotal AS subtotal_cents,
        tax_paid AS tax_paid_cents,
        order_total AS order_total_cents,
        {{ cents_to_dollars('subtotal') }} AS subtotal,
        tax_paid,
        {{ cents_to_dollars('order_total') }} AS order_total,
        ---------- timestamps
        CAST(
            ordered_at AS DATE
        ) AS order_date
    FROM
        source
)
SELECT
    *
FROM
    renamed
