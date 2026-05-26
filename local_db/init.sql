-- Initialization script for PostgreSQL Source Database
-- This will run on container startup to initialize tables and load CSV data

-- Create Transactions schema and table
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50),
    transaction_date TIMESTAMP,
    amount DECIMAL(10, 2),
    payment_method VARCHAR(50),
    product_category VARCHAR(100),
    store_location VARCHAR(100)
);

-- Note: In the actual implementation, we will use a COPY or import script 
-- to load the transactions.csv data into this table.
-- COPY transactions(transaction_id, customer_id, transaction_date, amount, payment_method, product_category, store_location)
-- FROM '/data/transactions.csv'
-- DELIMITER ','
-- CSV HEADER;
