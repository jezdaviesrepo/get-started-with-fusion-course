WITH orders AS (
    SELECT
        order_id,
        order_date,
        count_food_items,
        count_drink_items,
        order_total,
        CASE
            WHEN count_drink_items > 0 and count_food_items > 0 THEN 'mixed'
            WHEN count_food_items > 0 THEN 'food_only'
            WHEN count_drink_items > 0 THEN 'drink_only'
            ELSE 'unknown'
        END AS order_type
    FROM
        {{ ref('orders') }}
),
rollup AS (
    SELECT
        order_type,
        SUM(order_total) AS avg_profit_per_order,
        COUNT(order_id) AS order_counts
    FROM
        orders
    GROUP BY
        order_type
)
SELECT
    order_type,
    order_counts,
    avg_profit_per_order
FROM
    rollup
ORDER BY
    order_type;
