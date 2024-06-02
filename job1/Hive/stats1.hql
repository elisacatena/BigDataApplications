-- Drop existing tables if they exist
DROP TABLE IF EXISTS merged_data;
DROP TABLE IF EXISTS stocks;
DROP TABLE IF EXISTS intermediate_stock_prices;
DROP TABLE IF EXISTS report_finale_job1;
SET mapreduce.job.reduces=2;

-- Create the table for merged data
CREATE TABLE IF NOT EXISTS merged_data (
    `ticker` STRING,
    `open` FLOAT,
    `close` FLOAT,
    `adj_close` FLOAT,
    `low` FLOAT,
    `high` FLOAT,
    `volume` INT,
    `data` DATE,
    `exchange` STRING,
    `name` STRING,
    `sector` STRING,
    `industry` STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE
tblproperties("skip.header.line.count"="1");

-- Load merged data from hdfs
LOAD DATA INPATH 'hdfs:///user/hive/warehouse/input/merged_data.csv' OVERWRITE INTO TABLE merged_data;

CREATE TABLE IF NOT EXISTS stocks AS 
SELECT 
    ticker,
    name,
    close,
    volume,
    low,
    high,
    data,
    YEAR(data) AS anno
FROM
    merged_data;

WITH intermediate_stock_prices AS (
    SELECT 
        ticker,
        data,
        anno,
        close,
        low,
        high,
        volume,
        name,
        FIRST_VALUE(close) OVER (PARTITION BY ticker, anno ORDER BY data ASC) AS first_close,
        LAST_VALUE(close) OVER (PARTITION BY ticker, anno ORDER BY data ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_close
    FROM 
        stocks
),
report_finale_job1 AS (
    SELECT 
        ticker,
        name,
        anno,
        ROUND(((last_close - first_close) / first_close) * 100, 2) AS price_change_percent,
        ROUND(MIN(low), 2) AS min_price,
        ROUND(MAX(high), 2) AS max_price,
        ROUND(AVG(volume), 2) AS avg_volume
    FROM 
        intermediate_stock_prices
    GROUP BY 
        ticker, name, anno, first_close, last_close
    ORDER BY
        ticker, anno
)
INSERT OVERWRITE DIRECTORY 'hdfs:///user/hive/warehouse/report_finale_job1'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE
SELECT * FROM report_finale_job1;

-- Drop intermediate tables to clean up
DROP TABLE IF EXISTS merged_data;
DROP TABLE IF EXISTS stocks;
DROP TABLE IF EXISTS intermediate_stock_prices;
DROP TABLE IF EXISTS report_finale_job1;