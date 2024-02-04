-- Create a view from a parquet file
CREATE VIEW flights AS SELECT * FROM READ_PARQUET('s3://duckdb/flights.parquet');
-- Select the first 5 rows from the view
SELECT
    FL_DATE AS flight_date,
    DISTANCE AS distance,
    DEP_TIME AS departure_time,
    ARR_TIME AS arrival_time,
FROM flights LIMIT 5;
