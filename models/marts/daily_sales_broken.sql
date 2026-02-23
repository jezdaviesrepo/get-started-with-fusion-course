
WITH 
order_items AS (
    SELECT
        order_item_id,
        order_id,
        product_id,
        product_name,
        DATE_TRUNC(
            'day',
            order_date
        ) AS order_date,
        product_price,
        is_food_item AS is_food_item_flag,
        is_drink_item,
        supply_cost
    FROM
        {{ ref('order_items') }}
),

daily_rollup AS (
    SELECT
        order_date,
        product_id,
        SUM(product_price) AS daily_revenue,
        COUNT(order_item_id) AS items_sold
    FROM
        order_items
    GROUP BY
        order_date,
        product_id
),
leaderboard AS (
    SELECT
        order_date,
        product_id,
        daily_revenue,
        items_sold,
        ROW_NUMBER() OVER (
            PARTITION BY order_date
            ORDER BY
                daily_revenue ASC
        ) AS daily_rank
    FROM
        daily_rollup
)

SELECT
    *
FROM
    leaderboard
ORDER BY
    order_date,
    daily_rank
