-- Silver Layer: Normalized and Cleaned Transactions
-- This model cleans transaction values and deduplicates based on transaction_id

WITH bronze_data AS (
    SELECT * FROM {{ ref('stg_transactions') }}
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY transaction_id 
            ORDER BY transaction_timestamp DESC
        ) AS row_num
    FROM bronze_data
)

SELECT
    transaction_id,
    customer_id,
    transaction_timestamp,
    transaction_amount,
    UPPER(payment_method) AS payment_method,
    INITCAP(product_category) AS product_category,
    INITCAP(store_location) AS store_location
FROM deduplicated
WHERE row_num = 1
