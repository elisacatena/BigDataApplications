DROP TABLE IF EXISTS historical_stocks;
DROP TABLE IF EXISTS historical_stock_prices;
DROP TABLE IF EXISTS yearly_stock_stats;

-- Creazione della tabella per i dati storici dei prezzi
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

-- Caricamento dei dati storici dei prezzi
LOAD DATA INPATH 'hdfs:///user/hive/warehouse/historical_stock_prices.csv' OVERWRITE INTO TABLE historical_stock_prices;

-- Creazione della tabella per le informazioni sulle azioni
CREATE TABLE IF NOT EXISTS historical_stocks (
    `azione` STRING,
    `exchange` STRING,
    `nome` STRING,
    `sector` STRING,
    `industry` STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
tblproperties("skip.header.line.count"="1");

-- Caricamento delle informazioni sulle azioni
LOAD DATA INPATH 'hdfs:///user/hive/warehouse/historical_stocks.csv' OVERWRITE INTO TABLE historical_stocks;

-- Creazione di una tabella temporanea per le statistiche annuali
CREATE TABLE IF NOT EXISTS yearly_stock_stats AS
SELECT 
    azione,
    YEAR(data) AS anno,
    ROUND((MAX(close) - MIN(close)) / MIN(close) * 100, 2) AS price_change_percent,
    ROUND(MIN(low), 2) AS min_price,
    ROUND(MAX(high), 2) AS max_price,
    ROUND(AVG(volume), 2) AS avg_volume
FROM 
    historical_stock_prices
GROUP BY 
    azione, YEAR(data);

-- Calcolo delle statistiche per ogni azione
SELECT 
    y.azione AS azione,
    s.nome AS company_name, 
    y.anno AS anno,
    y.price_change_percent,
    y.min_price,
    y.max_price,
    y.avg_volume
FROM 
    yearly_stock_stats y
JOIN 
    historical_stocks s
ON 
    y.azione = s.azione
ORDER BY 
    y.azione, y.anno;