-- Drop existing tables if they exist
DROP TABLE IF EXISTS historical_stocks;
DROP TABLE IF EXISTS historical_stock_prices;
DROP TABLE IF EXISTS massimo_volume;
DROP TABLE IF EXISTS close_sums_temp;
DROP TABLE IF EXISTS variazione_percentuale_temp;
DROP TABLE IF EXISTS incremento_percentuale_azione;
DROP TABLE IF EXISTS massimo_incremento_percentuale;
DROP TABLE IF EXISTS report_finale_job2;

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

-- Identificare l'azione dell’industria con il maggior volume di transazioni nell’anno
CREATE TABLE IF NOT EXISTS massimo_volume AS
SELECT
    x.industry,
    x.anno,
    x.azione AS azione_massimo_volume,
    x.volume_transazioni
FROM (
    SELECT
        h.industry,
        YEAR(p.data) AS anno,
        p.azione,
        SUM(p.volume) AS volume_transazioni,
        ROW_NUMBER() OVER(PARTITION BY h.industry, YEAR(p.data) ORDER BY SUM(p.volume) DESC) AS row_num
    FROM
        historical_stocks h
    JOIN
        historical_stock_prices p
    ON
        h.azione = p.azione
    GROUP BY
        h.industry, YEAR(p.data), p.azione
) x
WHERE
    x.row_num = 1;

-- Calcolare la somma dei primi e degli ultimi prezzi di chiusura per ogni azione di ogni industria in ogni anno
CREATE TABLE IF NOT EXISTS close_sums_temp AS
SELECT
    h.industry,
    YEAR(p.data) AS anno,
    p.azione,
    FIRST_VALUE(p.close) OVER (PARTITION BY h.industry, YEAR(p.data), p.azione ORDER BY p.data) AS first_close,
    LAST_VALUE(p.close) OVER (PARTITION BY h.industry, YEAR(p.data), p.azione ORDER BY p.data ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_close
FROM
    historical_stocks h
JOIN
    historical_stock_prices p
ON
    h.azione = p.azione;

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
    anno,
    azione,
    ROUND((last_close - first_close) / first_close * 100, 2) AS incremento_percentuale
FROM
    close_sums_temp;

-- Identificare l'azione dell'industria con il maggior incremento percentuale nell'anno
CREATE TABLE IF NOT EXISTS massimo_incremento_percentuale AS
SELECT
    x.industry,
    x.anno,
    x.azione AS azione_massimo_incremento,
    x.incremento_percentuale
FROM (
    SELECT
        ipa.industry,
        ipa.anno,
        ipa.azione,
        ipa.incremento_percentuale,
        ROW_NUMBER() OVER(PARTITION BY ipa.industry, ipa.anno ORDER BY ipa.incremento_percentuale DESC) AS row_num
    FROM
        incremento_percentuale_azione ipa
) x
WHERE
    x.row_num = 1;

-- Creare il report finale
CREATE TABLE IF NOT EXISTS report_finale_job2 AS
SELECT DISTINCT
    hs.sector,
    mv.anno,
    mv.industry,
    vp.variazione_percentuale,
    mip.azione_massimo_incremento AS azione_maggior_incremento,
    mip.incremento_percentuale AS maggior_incremento_percentuale,
    mv.azione_massimo_volume AS azione_maggior_volume,
    mv.volume_transazioni AS maggior_volume
FROM
    massimo_volume mv
JOIN
    historical_stocks hs
ON
    mv.industry = hs.industry
JOIN
    variazione_percentuale_temp vp
ON
    mv.industry = vp.industry
    AND mv.anno = vp.anno
JOIN
    massimo_incremento_percentuale mip
ON
    mv.industry = mip.industry
    AND mv.anno = mip.anno
ORDER BY
    hs.sector,
    vp.variazione_percentuale DESC;

-- Drop temporary tables
DROP TABLE IF EXISTS historical_stocks;
DROP TABLE IF EXISTS historical_stock_prices;
DROP TABLE IF EXISTS massimo_volume;
DROP TABLE IF EXISTS close_sums_temp;
DROP TABLE IF EXISTS variazione_percentuale_temp;
DROP TABLE IF EXISTS incremento_percentuale_azione;
DROP TABLE IF EXISTS massimo_incremento_percentuale;
