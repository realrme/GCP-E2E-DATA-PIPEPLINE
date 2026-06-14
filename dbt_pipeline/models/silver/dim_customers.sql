-- ============================================================
-- SILVER LAYER: dim_customers
-- ============================================================
-- PURPOSE: One row per unique customer.
-- PRINCIPLE: Dimension tables answer "WHO?" — they are descriptive.
--
-- KEY DECISIONS:
--   1. DEDUPLICATION: A customer_id may appear in many transactions.
--      We take the LATEST snapshot of their loyalty_points
--      using ROW_NUMBER() partitioned by customer_id.
--
--   2. SURROGATE KEY: We generate a stable MD5 hash (customer_key)
--      instead of using the raw customer_id as PK. This protects
--      against upstream source changes (e.g., ID format changes).
--
--   3. MATERIALIZATION: table (full rebuild each run).
--      Dimension tables are typically small enough to fully reload.
-- ============================================================

{{
    config(
        materialized = 'table'
    )
}}

WITH stg AS (
    SELECT * FROM {{ ref('stg_grocery_transactions') }}
),

-- STEP 1: Keep only the most recent record per customer
-- (in case the same customer_id has different loyalty_points
--  across multiple transactions — we take the latest one)
ranked AS (
    SELECT
        customer_id,
        loyalty_points,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY transaction_date DESC
        ) AS row_num
    FROM stg
),

deduped AS (
    SELECT
        customer_id,
        loyalty_points
    FROM ranked
    WHERE row_num = 1
)

-- STEP 2: Generate surrogate key and apply standardization
SELECT
    -- Surrogate key: stable MD5 hash of customer_id
    {{ dbt_utils.generate_surrogate_key(['customer_id']) }} AS customer_key,

    customer_id,
    loyalty_points
FROM deduped
