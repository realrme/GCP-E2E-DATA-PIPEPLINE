-- ============================================================
-- GOLD LAYER: mart_sales_summary
-- ============================================================
-- PURPOSE: Business-ready aggregated summary for Looker Studio.
-- PRINCIPLE: Gold layer answers "HOW IS THE BUSINESS DOING?"
--
-- HOW THIS WORKS:
--   1. JOINs fact_grocery_transactions with dimension tables
--      (this is the classic Star Schema query pattern)
--   2. Groups by store + product + date to produce daily metrics
--   3. Materialized as VIEW — no storage cost,
--      Looker Studio always queries the freshest numbers.
--
-- METRICS PRODUCED:
--   - total_transactions : number of purchases per group
--   - total_items_sold   : total quantity across all purchases
--   - total_revenue      : gross revenue (before discounts)
--   - total_discount     : total discounts applied
--   - net_revenue        : actual revenue after discounts
--   - avg_basket_value   : average final_amount per transaction
--   - total_loyalty_pts  : loyalty points earned per group
-- ============================================================

{{
    config(
        materialized = 'view'
    )
}}

WITH fact AS (
    SELECT * FROM {{ ref('fact_grocery_transactions') }}
),

stores AS (
    SELECT * FROM {{ ref('dim_stores') }}
),

products AS (
    SELECT * FROM {{ ref('dim_products') }}
)

SELECT
    -- Dimensions (the "slice-and-dice" axes for dashboards)
    s.store_name,
    p.product_name,
    p.aisle                                  AS product_aisle,
    DATE(f.transaction_date)                 AS transaction_date,

    -- Volume metrics
    COUNT(f.transaction_key)                 AS total_transactions,
    SUM(f.quantity)                          AS total_items_sold,

    -- Revenue metrics
    ROUND(SUM(f.total_amount), 2)            AS total_revenue,
    ROUND(SUM(f.discount_amount), 2)         AS total_discount,
    ROUND(SUM(f.final_amount), 2)            AS net_revenue,
    ROUND(AVG(f.final_amount), 2)            AS avg_basket_value,

    -- Loyalty
    SUM(f.loyalty_points)                    AS total_loyalty_pts

FROM fact          AS f
INNER JOIN stores   AS s ON f.store_key   = s.store_key
INNER JOIN products AS p ON f.product_key = p.product_key

GROUP BY 1, 2, 3, 4
ORDER BY transaction_date DESC, net_revenue DESC
