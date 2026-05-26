-- Bronze Layer: Stage transactions
-- Selects from raw ingestion table and enforces basic types / column renaming

WITH source_data AS (
    SELECT * FROM {{ source('raw_source', 'transactions_raw') }}
)

SELECT
    CAST(transaction_id AS STRING) AS transaction_id,
    CAST(customer_id AS STRING) AS customer_id,
    CAST(transaction_date AS TIMESTAMP) AS transaction_timestamp,
    CAST(amount AS NUMERIC) AS transaction_amount,
    CAST(payment_method AS STRING) AS payment_method,
    CAST(product_category AS STRING) AS product_category,
    CAST(store_location AS STRING) AS store_location
FROM source_data
