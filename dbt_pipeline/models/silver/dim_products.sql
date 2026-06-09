-- ============================================================
-- SILVER LAYER: dim_products
-- ============================================================
-- PURPOSE: One row per unique product + aisle combination.
-- PRINCIPLE: Dimension tables answer "WHAT?" — they are descriptive.
--
-- KEY DECISIONS:
--   1. GRAIN: product_name + aisle forms the unique key.
--      The same product (e.g., "Onions") can appear in different
--      aisles (e.g., "Produce", "Canned Goods") — these are
--      treated as separate product-aisle combinations.
--
--   2. STANDARDIZATION:
--      - INITCAP(product_name) → consistent title casing
--      - INITCAP(aisle) → consistent title casing
--
--   3. AVERAGE UNIT PRICE: We store the average observed price
--      per product as a reference price. (Not used in fact table —
--      the fact table always stores the actual transaction price.)
--
--   4. MATERIALIZATION: table (full rebuild each run).
-- ============================================================

{{
    config(
        materialized = 'table'
    )
}}

WITH stg AS (
    SELECT * FROM {{ ref('stg_grocery_transactions') }}
),

-- STEP 1: Standardize and aggregate to one row per product+aisle
products AS (
    SELECT
        INITCAP(product_name) AS product_name,
        INITCAP(aisle)        AS aisle,
        AVG(unit_price)       AS avg_unit_price
    FROM stg
    WHERE product_name IS NOT NULL
    GROUP BY 1, 2
)

-- STEP 2: Generate surrogate key on the product+aisle grain
SELECT
    {{ dbt_utils.generate_surrogate_key(['product_name', 'aisle']) }} AS product_key,
    product_name,
    aisle,
    ROUND(avg_unit_price, 2) AS avg_unit_price
FROM products
