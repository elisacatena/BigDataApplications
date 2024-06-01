-- Drop existing tables if they exist
DROP TABLE IF EXISTS merged_data;
DROP TABLE IF EXISTS close_sums_temp;
DROP TABLE IF EXISTS industry_metrics;
DROP TABLE IF EXISTS stock_max_increment_filtered;
DROP TABLE IF EXISTS stock_max_volume;
DROP TABLE IF EXISTS report_finale_job2;
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

-- Load historical stock prices data
LOAD DATA INPATH 'hdfs:///user/hive/warehouse/input/merged_data.csv' OVERWRITE INTO TABLE merged_data;

-- Calculate first and last close prices, volume sums, and other required fields
CREATE TABLE IF NOT EXISTS close_sums_temp AS
SELECT
    sector,
    industry,
    YEAR(data) AS anno,
    ticker,
    FIRST_VALUE(`close`) OVER (PARTITION BY sector, industry, YEAR(data), ticker ORDER BY data) AS first_close,
    LAST_VALUE(`close`) OVER (PARTITION BY sector, industry, YEAR(data), ticker ORDER BY data ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_close,
    SUM(volume) OVER (PARTITION BY sector, industry, YEAR(data), ticker) AS total_volume
FROM
    merged_data;

-- Calculate the total first and last close prices for each industry per year
CREATE TABLE IF NOT EXISTS industry_metrics AS
SELECT
    sector,
    industry,
    anno,
    SUM(first_close) AS industry_first_total,
    SUM(last_close) AS industry_last_total
FROM close_sums_temp
GROUP BY sector, industry, anno;

-- Calculate the percentage increment for each stock within each industry and year
CREATE TABLE IF NOT EXISTS stock_max_increment_filtered AS
SELECT
    sector,
    industry,
    anno,
    ticker,
    (last_close - first_close) / first_close * 100 AS increment_percentage
FROM (
    SELECT
        sector,
        industry,
        anno,
        ticker,
        first_close,
        last_close,
        ROW_NUMBER() OVER (PARTITION BY sector, industry, anno ORDER BY (last_close - first_close) / first_close * 100 DESC) AS rank
    FROM close_sums_temp
) ranked
WHERE rank = 1;

-- Identify the stock with the highest volume within each industry and year
CREATE TABLE IF NOT EXISTS stock_max_volume AS
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

-- Create the final report table
CREATE TABLE IF NOT EXISTS report_finale_job2 AS
SELECT
    im.sector,
    im.industry,
    im.anno,
    ((im.industry_last_total - im.industry_first_total) / im.industry_first_total) * 100 AS industry_change_percentage,
    smif.ticker AS max_increment_ticker,
    smif.increment_percentage,
    smv.azione_massimo_volume AS max_volume_ticker,
    smv.volume_transazioni AS max_volume
FROM
    industry_metrics im
JOIN
    stock_max_increment_filtered smif ON im.sector = smif.sector AND im.industry = smif.industry AND im.anno = smif.anno
JOIN
    stock_max_volume smv ON im.sector = smv.sector AND im.industry = smv.industry AND im.anno = smv.anno
ORDER BY
    im.sector, industry_change_percentage DESC;

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
DROP TABLE IF EXISTS close_sums_temp;
DROP TABLE IF EXISTS industry_metrics;
DROP TABLE IF EXISTS stock_max_increment_filtered;
DROP TABLE IF EXISTS stock_max_volume;
