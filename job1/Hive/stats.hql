-- Drop existing tables if they exist
DROP TABLE IF EXISTS historical_stocks;
DROP TABLE IF EXISTS historical_stock_prices;
DROP TABLE IF EXISTS yearly_stock_stats;
DROP TABLE IF EXISTS intermediate_stock_prices;
DROP TABLE IF EXISTS report_finale_job1;

-- Create the table for historical stock prices data
CREATE TABLE IF NOT EXISTS historical_stock_prices (
    `azione` STRING,
    `open` FLOAT,
    `close` FLOAT,
    `adj_close` FLOAT,
    `low` FLOAT,
    `high` FLOAT,
    `volume` INT,
    `data` DATE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
tblproperties("skip.header.line.count"="1");

-- Load historical stock prices data
LOAD DATA INPATH 'hdfs:///user/hive/warehouse/input/historical_stock_prices1.csv' OVERWRITE INTO TABLE historical_stock_prices;

-- Create the table for stock information
CREATE TABLE IF NOT EXISTS historical_stocks (
    `azione` STRING,
    `exchange` STRING,
    `nome` STRING,
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

-- Load stock information data
LOAD DATA INPATH 'hdfs:///user/hive/warehouse/input/historical_stocks1.csv' OVERWRITE INTO TABLE historical_stocks;

-- Step 1: Create an intermediate table for window functions
CREATE TABLE IF NOT EXISTS intermediate_stock_prices AS
SELECT 
    azione,
    data,
    YEAR(data) AS anno,
    close,
    low,
    high,
    volume,
    FIRST_VALUE(close) OVER (PARTITION BY azione, YEAR(data) ORDER BY data ASC) AS first_close,
    LAST_VALUE(close) OVER (PARTITION BY azione, YEAR(data) ORDER BY data ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_close
FROM 
    historical_stock_prices;

-- Step 2: Create the table for yearly stock stats
CREATE TABLE IF NOT EXISTS yearly_stock_stats AS
SELECT 
    azione,
    anno,
    ROUND(((MAX(last_close) - MIN(first_close)) / MIN(first_close)) * 100, 2) AS price_change_percent,
    ROUND(MIN(low), 2) AS min_price,
    ROUND(MAX(high), 2) AS max_price,
    ROUND(AVG(volume), 2) AS avg_volume
FROM 
    intermediate_stock_prices
GROUP BY 
    azione, anno;

-- Step 3: Create the final report table
CREATE TABLE IF NOT EXISTS report_finale_job1 AS
SELECT 
    ys.azione AS azione,
    hs.nome AS nome_azienda,
    ys.anno AS anno,
    ys.price_change_percent AS variazione_percentuale_annuale,
    ys.min_price AS prezzo_minimo_annuale,
    ys.max_price AS prezzo_massimo_annuale,
    ys.avg_volume AS volume_medio_annuale
FROM 
    yearly_stock_stats ys
JOIN 
    historical_stocks hs
ON 
    ys.azione = hs.azione
ORDER BY 
    azione;

-- Drop intermediate tables to clean up
DROP TABLE IF EXISTS historical_stocks;
DROP TABLE IF EXISTS historical_stock_prices;
DROP TABLE IF EXISTS yearly_stock_stats;
DROP TABLE IF EXISTS intermediate_stock_prices;
