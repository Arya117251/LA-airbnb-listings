-- Airbnb LA Data Warehouse - Ingestion Script
-- File: ingestion.sql
-- Purpose: Initialize database schema and load raw staging data
-- ================================================================

-- ================================================================
-- SCHEMA SETUP
-- ================================================================

CREATE SCHEMA IF NOT EXISTS airbnb_dw;
SET search_path = airbnb_dw;

-- ================================================================
-- WELCOME MESSAGE
-- ================================================================

SELECT '================================================' AS message
UNION ALL SELECT 'Airbnb LA Data Warehouse Setup'
UNION ALL SELECT 'Database: snowflake-airbnb.duckdb'
UNION ALL SELECT 'Schema: airbnb_dw'
UNION ALL SELECT '================================================';

-- ================================================================
-- VERIFY SOURCE FILES
-- ================================================================

SELECT 'Verifying data files...' AS status;

SELECT 
    'neighbourhoods.csv' AS file,
    COUNT(*) AS row_count,
    'OK' AS status
FROM read_csv_auto('data/neighbourhoods.csv');

SELECT 
    'listings.csv' AS file,
    COUNT(*) AS row_count,
    'OK' AS status
FROM read_csv_auto('data/listings.csv');

SELECT 
    'reviews.csv' AS file,
    COUNT(*) AS row_count,
    'OK' AS status
FROM read_csv_auto('data/reviews.csv');

-- ================================================================
-- RAW DATA LOAD
-- ================================================================

SELECT 'Loading raw data into staging tables...' AS status;

-- ----------------------------
-- RAW: NEIGHBOURHOODS
-- ----------------------------
DROP TABLE IF EXISTS raw_neighbourhoods;

CREATE TABLE raw_neighbourhoods AS
SELECT *
FROM read_csv_auto('data/neighbourhoods.csv');

SELECT 
    'raw_neighbourhoods' AS table_name,
    COUNT(*) AS rows_loaded,
    'SUCCESS' AS status
FROM raw_neighbourhoods;

-- ----------------------------
-- RAW: LISTINGS
-- ----------------------------
DROP TABLE IF EXISTS raw_listings;

CREATE TABLE raw_listings AS
SELECT *
FROM read_csv_auto(
    'data/listings.csv',
    header = true,
    nullstr = ['', 'NULL', 'N/A']
);

SELECT 
    'raw_listings' AS table_name,
    COUNT(*) AS rows_loaded,
    'SUCCESS' AS status
FROM raw_listings;

-- ----------------------------
-- RAW: REVIEWS
-- ----------------------------
DROP TABLE IF EXISTS raw_reviews;

CREATE TABLE raw_reviews AS
SELECT *
FROM read_csv_auto('data/reviews.csv');

SELECT 
    'raw_reviews' AS table_name,
    COUNT(*) AS rows_loaded,
    'SUCCESS' AS status
FROM raw_reviews;

-- ================================================================
-- DATA QUALITY CHECKS
-- ================================================================

SELECT '================================================' AS message
UNION ALL SELECT 'Data Quality Summary'
UNION ALL SELECT '================================================';

-- Duplicate listing IDs
SELECT 
    'Duplicate listing IDs' AS check_name,
    COUNT(*) - COUNT(DISTINCT id) AS duplicate_count
FROM raw_listings;

-- Duplicate reviews (listing + date)
SELECT 
    'Duplicate reviews (listing + date)' AS check_name,
    COUNT(*) - COUNT(DISTINCT (listing_id || '_' || date)) AS duplicate_count
FROM raw_reviews;

-- Missing host_id
SELECT 
    'Listings with missing host_id' AS check_name,
    COUNT(*) AS count
FROM raw_listings
WHERE host_id IS NULL;

-- Missing neighbourhood
SELECT 
    'Listings with missing neighbourhood' AS check_name,
    COUNT(*) AS count
FROM raw_listings
WHERE neighbourhood IS NULL;

-- ================================================================
-- COMPLETION MESSAGE
-- ================================================================

SELECT '================================================' AS message
UNION ALL SELECT 'Ingestion Complete!'
UNION ALL SELECT 'Next Step: Create Dimension Tables';
