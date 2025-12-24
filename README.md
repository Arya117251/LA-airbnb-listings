🏠 Airbnb LA Data Warehouse & ETL Project
🚀 Project Description

This project is a Data Warehouse and ETL pipeline for Airbnb listings in Los Angeles. It transforms raw CSV files into a structured DuckDB warehouse with dimension and fact tables, making the data analytics-ready for BI tools like Power BI.

The goal is to provide a clean, queryable dataset for visualizations, analysis of listings, reviews, hosts, and locations—without needing raw CSVs every time.

🛠️ ETL Process Summary
1️⃣ Ingestion

Load raw CSV files into staging tables:

raw_neighbourhoods 🏘️

raw_listings 🏠

raw_reviews ✍️

Data quality checks performed:

Missing host IDs

Missing neighbourhoods

Duplicate listings and reviews


2️⃣ Dimension Tables

Cleaned, deduplicated, and enriched for analytics:

dim_location 🌍

Columns: locationKey, neighbourhoodGroup, neighbourhoodName

270 rows

dim_room_type 🛏️

Columns: roomTypeKey, roomType

4 room types: Entire home/apt, Hotel room, Private room, Shared room

dim_host 👤

Columns: hostKey, hostId, hostName, hostListingsCount

22,993 hosts

dim_date 📅

Columns: dateKey, fullDate, year, month, day, quarter, monthName, dayName, yearMonth

4,790 distinct review dates

3️⃣ Fact Tables

Final analytical tables ready for BI:

fact_listings 🏠

Combines listings with host, location, and room type dimensions

45,031 listings

fact_reviews ✍️

Links reviews to listing, host, location, and date dimensions

224,538 reviews

4️⃣ ETL Highlights

Used DuckDB for warehouse creation and ETL queries

Primary keys generated with ROW_NUMBER()

All joins are left joins to avoid missing data

Trimmed strings and cast numeric fields to correct types

CSVs stored in warehouse/ folder for reproducibility
