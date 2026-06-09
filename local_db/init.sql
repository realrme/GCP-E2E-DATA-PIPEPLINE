-- Clean up script for PostgreSQL Source Database
-- This will run on container startup to drop any old tables
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
