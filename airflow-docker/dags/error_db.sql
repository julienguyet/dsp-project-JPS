-- Create table for storing data quality errors
CREATE TABLE data_quality_errors (
    id SERIAL PRIMARY KEY,
    date TIMESTAMP,
    rule VARCHAR(255),
    rows INT,
    missing_values INT,
    percentage FLOAT,
    criticality INT
);
