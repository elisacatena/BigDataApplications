-- Drop existing tables if they exist
DROP TABLE IF EXISTS merged_data;
DROP TABLE IF EXISTS massimo_volume;
DROP TABLE IF EXISTS close_sums_temp;
DROP TABLE IF EXISTS variazione_percentuale_temp;
DROP TABLE IF EXISTS incremento_percentuale_azione;
DROP TABLE IF EXISTS massimo_incremento_percentuale;
DROP TABLE IF EXISTS report_finale_job2_1;

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
LOAD DATA INPATH 'hdfs:///user/hive/warehouse/input/merged_data1.csv' OVERWRITE INTO TABLE merged_data;

-- Create massimo_volume table to identify tickers with the highest volume for each industry and year
CREATE TABLE IF NOT EXISTS massimo_volume AS
SELECT
    sector,
    industry,
    anno,
    azione_massimo_volume,
    volume_transazioni
FROM (
    SELECT
        sector,
        industry,
        YEAR(data) AS anno,
        ticker AS azione_massimo_volume,
        SUM(volume) AS volume_transazioni,
        ROW_NUMBER() OVER (PARTITION BY sector, industry, YEAR(data) ORDER BY SUM(volume) DESC) AS row_num
    FROM
        merged_data
    GROUP BY
        sector, industry, YEAR(data), ticker
) ranked
WHERE
    row_num = 1;

-- Calcolare la somma dei primi e degli ultimi prezzi di chiusura per ogni azione di ogni industria in ogni anno
CREATE TABLE IF NOT EXISTS close_sums_temp AS
SELECT
    industry,
    ticker,
    YEAR(data) AS anno,
    ticker AS azione,
    FIRST_VALUE(close) OVER (PARTITION BY industry, YEAR(data), ticker ORDER BY data) AS first_close,
    LAST_VALUE(close) OVER (PARTITION BY industry, YEAR(data), ticker ORDER BY data ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_close
FROM
    merged_data;

-- Calcolare la variazione percentuale
CREATE TABLE IF NOT EXISTS variazione_percentuale_temp AS
SELECT
    industry,
    anno,
    ROUND((SUM(last_close) - SUM(first_close)) / SUM(first_close) * 100, 2) AS variazione_percentuale
FROM
    close_sums_temp
GROUP BY
    industry, anno;

-- Calcolare l'incremento percentuale per ogni azione
CREATE TABLE IF NOT EXISTS incremento_percentuale_azione AS
SELECT
    industry,
    ticker,
    anno,
    ROUND((last_close - first_close) / first_close * 100, 2) AS incremento_percentuale
FROM
    close_sums_temp;

-- Identificare l'azione dell'industria con il maggior incremento percentuale nell'anno
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
) x
WHERE
    row_num = 1;

-- Creare il report finale
CREATE TABLE IF NOT EXISTS report_finale_job2_1 AS
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
INSERT OVERWRITE DIRECTORY 'hdfs:///user/hive/warehouse/report_finale_job2_1'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE
SELECT * FROM report_finale_job2_1;

-- Drop temporary tables
DROP TABLE IF EXISTS merged_data;
DROP TABLE IF EXISTS massimo_volume;
DROP TABLE IF EXISTS close_sums_temp;
DROP TABLE IF EXISTS variazione_percentuale_temp;
DROP TABLE IF EXISTS incremento_percentuale_azione;
DROP TABLE IF EXISTS massimo_incremento_percentuale;