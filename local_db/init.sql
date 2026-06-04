-- Initialization script for PostgreSQL Source Database
-- This will run on container startup to initialize tables and load CSV data

-- Drop old tables if they exist
DROP TABLE IF EXISTS transactions;
DROP TABLE IF EXISTS olist_customers CASCADE;
DROP TABLE IF EXISTS olist_geolocation CASCADE;
DROP TABLE IF EXISTS olist_order_items CASCADE;
DROP TABLE IF EXISTS olist_order_payments CASCADE;
DROP TABLE IF EXISTS olist_order_reviews CASCADE;
DROP TABLE IF EXISTS olist_orders CASCADE;
DROP TABLE IF EXISTS olist_products CASCADE;
DROP TABLE IF EXISTS olist_sellers CASCADE;
DROP TABLE IF EXISTS product_category_name_translation CASCADE;

-- 1. olist_customers
CREATE TABLE olist_customers (
    customer_id VARCHAR(50) PRIMARY KEY,
    customer_unique_id VARCHAR(50),
    customer_zip_code_prefix VARCHAR(20),
    customer_city VARCHAR(100),
    customer_state VARCHAR(20)
);

-- 2. olist_geolocation
CREATE TABLE olist_geolocation (
    geolocation_zip_code_prefix VARCHAR(20),
    geolocation_lat DOUBLE PRECISION,
    geolocation_lng DOUBLE PRECISION,
    geolocation_city VARCHAR(100),
    geolocation_state VARCHAR(20)
);

-- 3. olist_order_items
CREATE TABLE olist_order_items (
    order_id VARCHAR(50),
    order_item_id INT,
    product_id VARCHAR(50),
    seller_id VARCHAR(50),
    shipping_limit_date TIMESTAMP,
    price DOUBLE PRECISION,
    freight_value DOUBLE PRECISION,
    PRIMARY KEY (order_id, order_item_id)
);

-- 4. olist_order_payments
CREATE TABLE olist_order_payments (
    order_id VARCHAR(50),
    payment_sequential INT,
    payment_type VARCHAR(50),
    payment_installments INT,
    payment_value DOUBLE PRECISION,
    PRIMARY KEY (order_id, payment_sequential)
);

-- 5. olist_order_reviews
CREATE TABLE olist_order_reviews (
    review_id VARCHAR(50),
    order_id VARCHAR(50),
    review_score INT,
    review_comment_title TEXT,
    review_comment_message TEXT,
    review_creation_date TIMESTAMP,
    review_answer_timestamp TIMESTAMP
);

-- 6. olist_orders
CREATE TABLE olist_orders (
    order_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50),
    order_status VARCHAR(50),
    order_purchase_timestamp TIMESTAMP,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP
);

-- 7. olist_products
CREATE TABLE olist_products (
    product_id VARCHAR(50) PRIMARY KEY,
    product_category_name VARCHAR(100),
    product_name_length DOUBLE PRECISION,
    product_description_length DOUBLE PRECISION,
    product_photos_qty DOUBLE PRECISION,
    product_weight_g DOUBLE PRECISION,
    product_length_cm DOUBLE PRECISION,
    product_height_cm DOUBLE PRECISION,
    product_width_cm DOUBLE PRECISION
);

-- 8. olist_sellers
CREATE TABLE olist_sellers (
    seller_id VARCHAR(50) PRIMARY KEY,
    seller_zip_code_prefix VARCHAR(20),
    seller_city VARCHAR(100),
    seller_state VARCHAR(20)
);

-- 9. product_category_name_translation
CREATE TABLE product_category_name_translation (
    product_category_name VARCHAR(100) PRIMARY KEY,
    product_category_name_english VARCHAR(100)
);

-- Load CSV data into tables using COPY
COPY olist_customers(customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state)
FROM '/olist_data/olist_customers_dataset.csv'
DELIMITER ','
CSV HEADER;

COPY olist_geolocation(geolocation_zip_code_prefix, geolocation_lat, geolocation_lng, geolocation_city, geolocation_state)
FROM '/olist_data/olist_geolocation_dataset.csv'
DELIMITER ','
CSV HEADER;

COPY olist_order_items(order_id, order_item_id, product_id, seller_id, shipping_limit_date, price, freight_value)
FROM '/olist_data/olist_order_items_dataset.csv'
DELIMITER ','
CSV HEADER;

COPY olist_order_payments(order_id, payment_sequential, payment_type, payment_installments, payment_value)
FROM '/olist_data/olist_order_payments_dataset.csv'
DELIMITER ','
CSV HEADER;

COPY olist_order_reviews(review_id, order_id, review_score, review_comment_title, review_comment_message, review_creation_date, review_answer_timestamp)
FROM '/olist_data/olist_order_reviews_dataset.csv'
DELIMITER ','
CSV HEADER;

COPY olist_orders(order_id, customer_id, order_status, order_purchase_timestamp, order_approved_at, order_delivered_carrier_date, order_delivered_customer_date, order_estimated_delivery_date)
FROM '/olist_data/olist_orders_dataset.csv'
DELIMITER ','
CSV HEADER;

COPY olist_products(product_id, product_category_name, product_name_length, product_description_length, product_photos_qty, product_weight_g, product_length_cm, product_height_cm, product_width_cm)
FROM '/olist_data/olist_products_dataset.csv'
DELIMITER ','
CSV HEADER;

COPY olist_sellers(seller_id, seller_zip_code_prefix, seller_city, seller_state)
FROM '/olist_data/olist_sellers_dataset.csv'
DELIMITER ','
CSV HEADER;

COPY product_category_name_translation(product_category_name, product_category_name_english)
FROM '/olist_data/product_category_name_translation.csv'
DELIMITER ','
CSV HEADER;
