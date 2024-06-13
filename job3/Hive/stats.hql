-- Drop existing tables
DROP TABLE IF EXISTS merged_data;
DROP TABLE IF EXISTS stocks;
DROP TABLE IF EXISTS intermediate_stock_prices;
DROP TABLE IF EXISTS report_finale_job3;
DROP TABLE IF EXISTS report_finale_triennali;
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
    close,
    data,
    YEAR(data) AS anno
FROM
    merged_data
WHERE
    YEAR(data) >= 2000;

CREATE TABLE IF NOT EXISTS intermediate_stock_prices AS 
    SELECT 
        ticker,
        data,
        anno,
        FIRST_VALUE(close) OVER (PARTITION BY ticker, anno ORDER BY data ASC) AS first_close,
        LAST_VALUE(close) OVER (PARTITION BY ticker, anno ORDER BY data ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_close
    FROM 
        stocks;

CREATE TABLE IF NOT EXISTS report_finale_job3 AS 
    SELECT 
        ticker,
        anno,
        ROUND(((last_close - first_close) / first_close) * 100, 2) AS price_change_percent    
    FROM 
        intermediate_stock_prices
    GROUP BY 
        ticker, anno, first_close, last_close
    ORDER BY
        ticker, anno;
        
CREATE TABLE IF NOT EXISTS report_finale_triennali AS 
    SELECT
        t1.ticker,
        t1.anno,
        CONCAT(t1.anno, ',', t2.anno, ',', t3.anno) AS anni,
        CONCAT(t1.price_change_percent, ',', t2.price_change_percent, ',', t3.price_change_percent) AS variazioni
    FROM
        report_finale_job3 t1
    JOIN
        report_finale_job3 t2 ON t1.ticker = t2.ticker AND t2.anno = t1.anno + 1
    JOIN
        report_finale_job3 t3 ON t1.ticker = t3.ticker AND t3.anno = t1.anno + 2
    ORDER BY
        t1.ticker, t1.anno;

INSERT OVERWRITE DIRECTORY 'hdfs:///user/hive/warehouse/report_finale_triennali'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE
SELECT ticker, anni, variazioni FROM report_finale_triennali;

-- Drop intermediate tables to clean up
DROP TABLE IF EXISTS merged_data;
DROP TABLE IF EXISTS stocks;
DROP TABLE IF EXISTS intermediate_stock_prices;
DROP TABLE IF EXISTS report_finale_job3;
DROP TABLE IF EXISTS report_finale_triennali;