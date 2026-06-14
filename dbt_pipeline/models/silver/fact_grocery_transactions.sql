-- ============================================================
-- SILVER LAYER: fact_grocery_transactions (INCREMENTAL)
-- ============================================================
-- PURPOSE: One row per grocery transaction line item.
-- PRINCIPLE: Fact tables answer "WHAT HAPPENED?" — they store
--   measurable business events with numeric metrics.
--
-- KEY DECISIONS:
--   1. INCREMENTAL MATERIALIZATION:
--      This is the most important pattern for large fact tables.
--      Instead of rebuilding ALL rows every run, we only process
--      rows WHERE ingestion_date >= last run date.
--
--      HOW IT WORKS:
--      - First run: loads ALL rows (no filter applied)
--      - Subsequent runs: loads only NEW rows since last run
--      - {{ this }} is a dbt macro that references the existing table
--
--   2. UNIQUE KEY: A surrogate key generated from all business
--      attributes that make a transaction unique. This allows
--      dbt to MERGE (upsert) instead of blindly appending,
--      preventing duplicate rows if the DAG re-runs.
--
--   3. FOREIGN KEYS → DIMENSION KEYS:
--      We JOIN to dimension tables to resolve surrogate keys.
--      This enforces referential integrity — if a transaction
--      references a store not in dim_stores, it gets filtered out.
--
--   4. DATA QUALITY FILTER:
--      We only keep rows where final_amount >= 0.
--      Negative final_amount = discount exceeded price (data error).
-- ============================================================

{{
    config(
        materialized    = 'incremental',
        unique_key      = 'transaction_key',
        partition_by    = {
            'field': 'transaction_date',
            'data_type': 'timestamp',
            'granularity': 'day'
        },
        cluster_by      = ['store_key', 'product_key']
    )
}}

WITH stg AS (
    SELECT * FROM {{ ref('stg_grocery_transactions') }}

    -- ✅ INCREMENTAL FILTER:
    -- On subsequent runs, only process new partitions.
    -- On first run, this block is skipped (loads everything).
    {% if is_incremental() %}
        WHERE ingestion_date > (SELECT MAX(ingestion_date) FROM {{ this }})
    {% endif %}
),

-- Resolve surrogate keys from dimension tables
dim_c AS (SELECT * FROM {{ ref('dim_customers') }}),
dim_s AS (SELECT * FROM {{ ref('dim_stores') }}),
dim_p AS (SELECT * FROM {{ ref('dim_products') }})

SELECT
    -- ✅ SURROGATE KEY for this fact row (unique per transaction event)
    {{ dbt_utils.generate_surrogate_key([
        'stg.customer_id',
        'stg.store_name',
        'stg.transaction_date',
        'stg.product_name',
        'stg.aisle'
    ]) }} AS transaction_key,

    -- Foreign keys linking to dimension tables
    dim_c.customer_key,
    dim_s.store_key,
    dim_p.product_key,

    -- Degenerate dimensions (useful attributes kept directly in fact)
    stg.transaction_date,

    -- Measures (the numeric business facts)
    stg.quantity,
    stg.unit_price,
    stg.total_amount,
    stg.discount_amount,
    stg.final_amount,
    stg.loyalty_points,

    -- Metadata
    stg.ingestion_date

FROM stg

-- INNER JOINs enforce referential integrity:
-- Rows without matching dimension records are excluded
INNER JOIN dim_c ON stg.customer_id = dim_c.customer_id
INNER JOIN dim_s ON INITCAP(stg.store_name) = dim_s.store_name
INNER JOIN dim_p
    ON INITCAP(stg.product_name) = dim_p.product_name
    AND INITCAP(stg.aisle) = dim_p.aisle

-- ✅ DATA QUALITY FILTER:
-- Exclude transactions with negative final_amount (bad discount data)
WHERE stg.final_amount >= 0
