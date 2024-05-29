-- Drop existing tables if they exist
DROP TABLE IF EXISTS merged_data;
DROP TABLE IF EXISTS massimo_volume;
DROP TABLE IF EXISTS close_sums_temp;
DROP TABLE IF EXISTS variazione_percentuale_temp;
DROP TABLE IF EXISTS incremento_percentuale_azione;
DROP TABLE IF EXISTS massimo_incremento_percentuale;
DROP TABLE IF EXISTS report_finale_job2;
SET mapreduce.job.reduces=2;

-- Create the table for historical stock prices data
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

-- Load historical stock prices data
LOAD DATA INPATH 'hdfs:///user/hive/warehouse/input/merged_data.csv' OVERWRITE INTO TABLE merged_data;

-- Calculate first and last close prices, volume sums, and other required fields
CREATE TABLE IF NOT EXISTS close_sums_temp AS
SELECT
    sector,
    industry,
    ticker,
    YEAR(data) AS anno,
    FIRST_VALUE(close) OVER (PARTITION BY industry, YEAR(data), ticker ORDER BY data ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_close,
    LAST_VALUE(close) OVER (PARTITION BY industry, YEAR(data), ticker ORDER BY data ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_close,
    SUM(volume) OVER (PARTITION BY sector, industry, YEAR(data), ticker) AS total_volume
FROM
    merged_data;

-- Create massimo_volume table to identify tickers with the highest volume for each industry and year
CREATE TABLE IF NOT EXISTS massimo_volume AS
SELECT
    sector,
    industry,
    anno,
    ticker AS azione_massimo_volume,
    total_volume AS volume_transazioni
FROM (
    SELECT
        sector,
        industry,
        anno,
        ticker,
        total_volume,
        ROW_NUMBER() OVER (PARTITION BY sector, industry, anno ORDER BY total_volume DESC) AS row_num
    FROM
        close_sums_temp
) ranked
WHERE
    row_num = 1;

-- Calculate the percentage change for each industry and year
CREATE TABLE IF NOT EXISTS variazione_percentuale_temp AS
SELECT
    industry,
    anno,
    ROUND((SUM(last_close) - SUM(first_close)) / SUM(first_close) * 100, 2) AS variazione_percentuale
FROM
    close_sums_temp
GROUP BY
    industry, anno;

-- Calculate the percentage increase for each ticker within each industry and year
CREATE TABLE IF NOT EXISTS incremento_percentuale_azione AS
SELECT
    industry,
    ticker,
    anno,
    ROUND((last_close - first_close) / first_close * 100, 2) AS incremento_percentuale
FROM
    close_sums_temp;

-- Identify the ticker with the highest percentage increase within each industry and year
CREATE TABLE IF NOT EXISTS massimo_incremento_percentuale AS
SELECT
    industry,
    anno,
    ticker,
    incremento_percentuale
FROM (
    SELECT
        industry,
        anno,
        ticker,
        incremento_percentuale,
        ROW_NUMBER() OVER(PARTITION BY industry, anno ORDER BY incremento_percentuale DESC) AS row_num
    FROM
        incremento_percentuale_azione
) ranked
WHERE
    row_num = 1;

-- Create the final report table
CREATE TABLE IF NOT EXISTS report_finale_job2 AS
SELECT
    mv.sector,
    mv.anno,
    mv.industry,
    vp.variazione_percentuale,
    mip.ticker AS azione_massimo_incremento,
    mip.incremento_percentuale AS incremento_percentuale,
    mv.azione_massimo_volume AS azione_massimo_volume,
    mv.volume_transazioni AS volume_transazioni
FROM
    massimo_volume mv
JOIN
    variazione_percentuale_temp vp
ON 
    mv.industry = vp.industry AND mv.anno = vp.anno
JOIN
    massimo_incremento_percentuale mip 
ON
    mip.industry = vp.industry AND mip.anno = vp.anno
ORDER BY
    mv.sector, incremento_percentuale DESC;

-- Step 3: Export the final report to a CSV file
INSERT OVERWRITE DIRECTORY 'hdfs:///user/hive/warehouse/report_finale_job2'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE
SELECT * FROM report_finale_job2;

-- Drop temporary tables
DROP TABLE IF EXISTS merged_data;
DROP TABLE IF EXISTS massimo_volume;
DROP TABLE IF EXISTS close_sums_temp;
DROP TABLE IF EXISTS variazione_percentuale_temp;
DROP TABLE IF EXISTS incremento_percentuale_azione;
DROP TABLE IF EXISTS massimo_incremento_percentuale;
