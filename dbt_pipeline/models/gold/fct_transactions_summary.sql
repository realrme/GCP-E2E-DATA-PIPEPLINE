-- Gold Layer: Aggregated Transaction Summary
-- Materialized as a View to be exposed to Looker Studio

WITH silver_data AS (
    SELECT * FROM {{ ref('int_transactions_normalized') }}
)

SELECT
    store_location,
    product_category,
    payment_method,
    DATE(transaction_timestamp) AS transaction_date,
    COUNT(transaction_id) AS total_transactions,
    SUM(transaction_amount) AS total_revenue,
    AVG(transaction_amount) AS average_order_value
FROM silver_data
GROUP BY 1, 2, 3, 4
