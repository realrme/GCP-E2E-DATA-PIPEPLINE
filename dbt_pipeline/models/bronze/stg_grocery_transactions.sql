-- ============================================================
-- BRONZE LAYER: Staging Grocery Transactions
-- ============================================================
-- PURPOSE: 1-to-1 mirror of the raw BigQuery table.
-- PRINCIPLE: No business logic here — only:
--   1. Rename columns to snake_case conventions
--   2. Cast types explicitly (never trust autodetect)
--   3. Expose the source to downstream models via {{ source() }}
--
-- MATERIALIZATION: table
--   We rebuild this fully on each run — it's cheap because it's
--   just a thin wrapper over the partitioned raw table.
-- ============================================================

WITH source AS (
    SELECT * FROM {{ source('bronze', 'raw_grocery_transactions') }}
)

SELECT
    -- Customer context
    CAST(customer_id       AS STRING)    AS customer_id,

    -- Store context
    CAST(store_name        AS STRING)    AS store_name,

    -- Transaction timing
    CAST(transaction_date  AS TIMESTAMP) AS transaction_date,

    -- Product context
    CAST(aisle             AS STRING)    AS aisle,
    CAST(product_name      AS STRING)    AS product_name,

    -- Transaction amounts
    CAST(quantity          AS INT64)     AS quantity,
    CAST(unit_price        AS NUMERIC)   AS unit_price,
    CAST(total_amount      AS NUMERIC)   AS total_amount,
    CAST(discount_amount   AS NUMERIC)   AS discount_amount,
    CAST(final_amount      AS NUMERIC)   AS final_amount,

    -- Loyalty
    CAST(loyalty_points    AS INT64)     AS loyalty_points,

    -- Bronze metadata columns (from ingestion script)
    _ingested_at,
    ingestion_date

FROM source
