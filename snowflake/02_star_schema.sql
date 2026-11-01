USE DATABASE TAXI_DB;
USE SCHEMA PROD;

-- Reference Dimensions
CREATE OR REPLACE TABLE DIM_ZONE (
    location_id INT PRIMARY KEY,
    borough VARCHAR(50),
    zone_name VARCHAR(100),
    service_zone VARCHAR(50)
);

CREATE OR REPLACE TABLE DIM_PAYMENT_TYPE (
    payment_type_id INT PRIMARY KEY,
    description VARCHAR(50)
);

-- Fact Table
CREATE OR REPLACE TABLE FACT_TRIPS (
    trip_id VARCHAR(100) PRIMARY KEY,
    vendor_id INT,
    pickup_datetime TIMESTAMP_NTZ,
    dropoff_datetime TIMESTAMP_NTZ,
    passenger_count INT,
    trip_distance FLOAT,
    fare_amount FLOAT,
    total_amount FLOAT,
    pickup_location_id INT,
    dropoff_location_id INT,
    payment_type INT,
    ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Staging table for incoming batches before idempotent merge
CREATE OR REPLACE TABLE TAXI_DB.STAGING.STAGING_FACT_TRIPS LIKE FACT_TRIPS;

-- Hourly Aggregated Mart
CREATE OR REPLACE TABLE AGG_HOURLY_ZONE (
    pickup_location_id INT,
    hour_start TIMESTAMP_NTZ,
    hour_end TIMESTAMP_NTZ,
    total_trips INT,
    total_revenue FLOAT,
    avg_distance FLOAT,
    PRIMARY KEY (pickup_location_id, hour_start)
);

-- Seed Payment Dimension
INSERT INTO DIM_PAYMENT_TYPE (payment_type_id, description) VALUES
(1, 'Credit Card'), 
(2, 'Cash'), 
(3, 'No Charge'),
(4, 'Dispute'), 
(5, 'Unknown'), 
(6, 'Voided Trip');
