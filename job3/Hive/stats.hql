-- Drop existing tables
DROP TABLE IF EXISTS merged_data;
DROP TABLE IF EXISTS stocks;
DROP TABLE IF EXISTS intermediate_stock_prices;
DROP TABLE IF EXISTS report_finale_job3;
DROP TABLE IF EXISTS report_finale_triennali;
DROP TABLE IF EXISTS filtered_report;

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
LOAD DATA LOCAL INPATH '/Users/elisacatena/git/BigDataApplications/input/merged_data.csv' OVERWRITE INTO TABLE merged_data;

CREATE TABLE IF NOT EXISTS stocks AS 
SELECT 
    ticker,
    name,
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
        close,
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

-- Create a table to filter report_finale_triennali to keep only rows with duplicate anni and variazioni
CREATE TABLE IF NOT EXISTS duplicate_anni_variazioni AS
SELECT 
    anni, 
    variazioni 
FROM 
    report_finale_triennali
GROUP BY 
    anni, 
    variazioni 
HAVING 
    COUNT(*) > 1;

CREATE TABLE IF NOT EXISTS filtered_report AS
SELECT 
    rft.ticker, 
    rft.anni, 
    rft.variazioni
FROM 
    report_finale_triennali rft
JOIN
    duplicate_anni_variazioni dav 
    ON rft.anni = dav.anni AND rft.variazioni = dav.variazioni
ORDER BY
    rft.ticker, rft.anni;

INSERT OVERWRITE LOCAL DIRECTORY '/Users/elisacatena/Desktop/out'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE
SELECT ticker, anni, variazioni FROM filtered_report;

-- Drop intermediate tables to clean up
DROP TABLE IF EXISTS merged_data;
DROP TABLE IF EXISTS stocks;
DROP TABLE IF EXISTS intermediate_stock_prices;
DROP TABLE IF EXISTS report_finale_job3;
DROP TABLE IF EXISTS report_finale_triennali;
DROP TABLE IF EXISTS duplicate_anni_variazioni;
DROP TABLE IF EXISTS filtered_report;
