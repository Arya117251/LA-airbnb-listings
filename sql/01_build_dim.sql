-- =========================
-- Airbnb LA Data Warehouse - DuckDB Version (Drop & Rebuild, Fixed for numeric IDs)
-- =========================

-- -------------------------
-- DROP EXISTING TABLES
-- -------------------------
DROP TABLE IF EXISTS fact_reviews;
DROP TABLE IF EXISTS fact_listings;
DROP TABLE IF EXISTS dim_date;
DROP TABLE IF EXISTS dim_host;
DROP TABLE IF EXISTS dim_room_type;
DROP TABLE IF EXISTS dim_location;

-- -------------------------
-- DIM_LOCATION
-- -------------------------
CREATE TABLE dim_location (
    locationKey INTEGER PRIMARY KEY,
    neighbourhoodGroup VARCHAR,
    neighbourhoodName VARCHAR
);

INSERT INTO dim_location (locationKey, neighbourhoodGroup, neighbourhoodName)
SELECT ROW_NUMBER() OVER (ORDER BY neighbourhood_group, neighbourhood) AS locationKey,
       neighbourhood_group,
       neighbourhood
FROM raw_neighbourhoods;

-- -------------------------
-- DIM_ROOM_TYPE
-- -------------------------
CREATE TABLE dim_room_type (
    roomTypeKey INTEGER PRIMARY KEY,
    roomType VARCHAR
);

INSERT INTO dim_room_type (roomTypeKey, roomType)
SELECT ROW_NUMBER() OVER (ORDER BY room_type) AS roomTypeKey,
       room_type
FROM (SELECT DISTINCT room_type 
      FROM raw_listings 
      WHERE room_type IS NOT NULL);

-- -------------------------
-- DIM_HOST
-- -------------------------
CREATE TABLE dim_host (
    hostKey INTEGER PRIMARY KEY,
    hostId BIGINT,
    hostName VARCHAR,
    hostListingsCount INTEGER
);

INSERT INTO dim_host (hostKey, hostId, hostName, hostListingsCount)
SELECT ROW_NUMBER() OVER (ORDER BY host_id) AS hostKey,
       host_id,
       MAX(host_name) AS hostName,
       MAX(CAST(calculated_host_listings_count AS INTEGER)) AS hostListingsCount
FROM raw_listings
WHERE host_id IS NOT NULL
GROUP BY host_id;

-- -------------------------
-- DIM_DATE
-- -------------------------
CREATE TABLE dim_date (
    dateKey INTEGER PRIMARY KEY,
    fullDate DATE,
    year INTEGER,
    month INTEGER,
    day INTEGER,
    quarter INTEGER,
    monthName VARCHAR,
    dayName VARCHAR,
    yearMonth VARCHAR
);

INSERT INTO dim_date (dateKey, fullDate, year, month, day, quarter, monthName, dayName, yearMonth)
SELECT ROW_NUMBER() OVER (ORDER BY date) AS dateKey,
       CAST(date AS DATE) AS fullDate,
       EXTRACT(YEAR FROM date) AS year,
       EXTRACT(MONTH FROM date) AS month,
       EXTRACT(DAY FROM date) AS day,
       EXTRACT(QUARTER FROM date) AS quarter,
       strftime('%B', date) AS monthName,
       strftime('%A', date) AS dayName,
       strftime('%Y-%m', date) AS yearMonth
FROM (SELECT DISTINCT date FROM raw_reviews WHERE date IS NOT NULL);

-- -------------------------
-- FACT_LISTINGS
-- -------------------------
CREATE TABLE fact_listings (
    listingKey INTEGER PRIMARY KEY,
    listingId BIGINT,
    hostKey INTEGER,
    locationKey INTEGER,
    roomTypeKey INTEGER,
    price DECIMAL(10,2),
    minimumNights INTEGER,
    availability365 INTEGER,
    numberOfReviews INTEGER,
    numberOfReviewsLTM INTEGER,
    reviewsPerMonth DECIMAL(10,2),
    lastReviewDate DATE
);

INSERT INTO fact_listings (listingKey, listingId, hostKey, locationKey, roomTypeKey, price,
                           minimumNights, availability365, numberOfReviews, numberOfReviewsLTM,
                           reviewsPerMonth, lastReviewDate)
SELECT ROW_NUMBER() OVER (ORDER BY rl.id) AS listingKey,
       rl.id AS listingId,
       h.hostKey,
       l.locationKey,
       rt.roomTypeKey,
       CAST(rl.price AS DECIMAL(10,2)),
       CAST(rl.minimum_nights AS INTEGER),
       CAST(rl.availability_365 AS INTEGER),
       CAST(rl.number_of_reviews AS INTEGER),
       CAST(rl.number_of_reviews_ltm AS INTEGER),
       CAST(rl.reviews_per_month AS DECIMAL(10,2)),
       CAST(rl.last_review AS DATE)
FROM raw_listings rl
LEFT JOIN dim_host h ON rl.host_id = h.hostId
LEFT JOIN dim_location l
       ON rl.neighbourhood_group = l.neighbourhoodGroup
      AND rl.neighbourhood = l.neighbourhoodName
LEFT JOIN dim_room_type rt ON rl.room_type = rt.roomType;

-- -------------------------
-- FACT_REVIEWS
-- -------------------------
CREATE TABLE fact_reviews (
    reviewKey INTEGER PRIMARY KEY,
    dateKey INTEGER,
    hostKey INTEGER,
    locationKey INTEGER,
    listingId BIGINT,
    reviewCount INTEGER DEFAULT 1
);

INSERT INTO fact_reviews (reviewKey, dateKey, hostKey, locationKey, listingId, reviewCount)
SELECT
    ROW_NUMBER() OVER (ORDER BY rr.listing_id, rr.date) AS reviewKey,
    d.dateKey,
    h.hostKey,
    l.locationKey,
    rr.listing_id AS listingId,
    1 AS reviewCount
FROM raw_reviews rr
LEFT JOIN raw_listings rl ON rr.listing_id = rl.id
LEFT JOIN dim_host h ON rl.host_id = h.hostId
LEFT JOIN dim_location l
       ON rl.neighbourhood_group = l.neighbourhoodGroup
      AND rl.neighbourhood = l.neighbourhoodName
LEFT JOIN dim_date d ON CAST(rr.date AS DATE) = d.fullDate
WHERE rr.listing_id IS NOT NULL
  AND rr.date IS NOT NULL;
