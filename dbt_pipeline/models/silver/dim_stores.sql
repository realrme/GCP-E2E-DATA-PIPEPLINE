-- ============================================================
-- SILVER LAYER: dim_stores
-- ============================================================
-- PURPOSE: One row per unique store.
-- PRINCIPLE: Dimension tables answer "WHERE?" — they are descriptive.
--
-- KEY DECISIONS:
--   1. STANDARDIZATION: store_name comes in mixed casing.
--      INITCAP() normalizes to Title Case (e.g., "greengrocer plaza"
--      → "Greengrocer Plaza") so that GROUP BY works correctly
--      and dashboards look clean.
--
--   2. DEDUPLICATION: Using DISTINCT since store_name is the
--      only attribute — no ranking needed.
--
--   3. SURROGATE KEY: MD5 hash of normalized store_name.
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

-- STEP 1: Standardize store names and deduplicate
stores AS (
    SELECT DISTINCT
        INITCAP(store_name) AS store_name
    FROM stg
    WHERE store_name IS NOT NULL
)

-- STEP 2: Generate surrogate key
SELECT
    {{ dbt_utils.generate_surrogate_key(['store_name']) }} AS store_key,
    store_name
FROM stores
