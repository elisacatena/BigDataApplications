-- Elimina la tabella merged_data se esiste
DROP TABLE IF EXISTS merged_data;
DROP TABLE IF EXISTS stocks;
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

-- Carica i dati nella tabella merged_data
LOAD DATA INPATH 'hdfs:///user/hive/warehouse/input/merged_data.csv' OVERWRITE INTO TABLE merged_data;

CREATE TABLE IF NOT EXISTS stocks AS 
SELECT 
    ticker,
    close,
    volume,
    data,
    YEAR(data) AS anno,
    sector,
    industry
FROM
    merged_data;

WITH yearly_stats AS (
    SELECT
        sector,
        industry,
        anno,
        ticker,
        FIRST_VALUE(close) OVER (PARTITION BY sector, industry, ticker, anno ORDER BY data) AS first_close,
        LAST_VALUE(close) OVER (PARTITION BY sector, industry, ticker, anno ORDER BY data ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_close,
        SUM(volume) OVER (PARTITION BY sector, industry, anno, ticker) AS total_volume
    FROM stocks
),
aggregated_metrics AS (
    SELECT
        sector,
        industry,
        anno,
        SUM(first_close) AS total_initial_close,
        SUM(last_close) AS total_final_close
    FROM yearly_stats
    GROUP BY sector, industry, anno
),
max_increment_ticker AS (
    SELECT
        sector,
        industry,
        anno,
        ticker,
        ((last_close - first_close) / first_close) * 100 AS increment_percentage,
        ROW_NUMBER() OVER (PARTITION BY sector, industry, anno ORDER BY ((last_close - first_close) / first_close) * 100 DESC) AS rank
    FROM yearly_stats
),
top_increment_ticker AS (
    SELECT
        sector,
        industry,
        anno,
        ticker,
        increment_percentage
    FROM max_increment_ticker
    WHERE rank = 1
),
top_volume_ticker AS (
    SELECT
        sector,
        industry,
        anno,
        ticker AS max_volume_ticker,
        total_volume AS max_volume
    FROM (
        SELECT
            sector,
            industry,
            anno,
            ticker,
            total_volume,
            ROW_NUMBER() OVER (PARTITION BY sector, industry, anno ORDER BY total_volume DESC) AS row_num
        FROM
            yearly_stats
    ) ranked
    WHERE
        row_num = 1
)
INSERT OVERWRITE DIRECTORY 'hdfs:///user/hive/warehouse/report_finale_job2'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE
SELECT
    am.sector,
    am.anno,
    am.industry, 
    ROUND((((am.total_final_close - am.total_initial_close) / am.total_initial_close) * 100),2) AS industry_price_change,
    tit.ticker AS max_increase_ticker,
    ROUND(tit.increment_percentage,2),
    tvt.max_volume_ticker,
    tvt.max_volume
FROM
    aggregated_metrics am
JOIN
    top_increment_ticker tit ON am.sector = tit.sector AND am.industry = tit.industry AND am.anno = tit.anno
JOIN
    top_volume_ticker tvt ON am.sector = tvt.sector AND am.industry = tvt.industry AND am.anno = tvt.anno
ORDER BY
    am.sector, industry_price_change DESC;

DROP TABLE IF EXISTS merged_data;
DROP TABLE IF EXISTS stocks;