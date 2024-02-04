-- Create a view from a parquet file
CREATE VIEW flights AS SELECT * FROM READ_PARQUET('s3://duckdb/flights.parquet');
-- Select the first 5 rows from the view
SELECT * FROM flights LIMIT 5;
