-- Create a view from a CSV file
CREATE VIEW public_chauffeurs AS(
    SELECT *
    FROM READ_CSV('s3://duckdb/public_chauffeurs.csv', AUTO_DETECT=TRUE)
);
-- Get the licenses expired in the last month, and write them to a new portioned parquet file
COPY (
    SELECT
        EXPIRES[4:] AS EXPIRATION_YEAR,
        EXPIRES[:2] AS EXPIRATION_MONTH,
        *
    FROM public_chauffeurs
    WHERE EXPIRES = '{{ logical_date.strftime("%m/%Y") }}'
)
TO 's3://duckdb/expired_licenses'
(FORMAT PARQUET, PARTITION_BY (EXPIRATION_YEAR, EXPIRATION_MONTH), OVERWRITE_OR_IGNORE);

-- Get the count of licenses expired in the last month to return it as an XCom
SELECT COUNT(*) AS expired_licenses_count
FROM READ_PARQUET('s3://duckdb/expired_licenses/*/*/*.parquet')
WHERE EXPIRATION_YEAR = '{{ logical_date.strftime("%Y") }}'
    AND EXPIRATION_MONTH = '{{ logical_date.strftime("%m") }}';
