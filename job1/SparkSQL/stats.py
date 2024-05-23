from pyspark.sql import SparkSession

# Creazione della sessione Spark
spark = SparkSession.builder \
    .appName("Stock Statistics SQL") \
    .getOrCreate()

# Percorsi dei file su HDFS
stock_prices_path = "hdfs://localhost:9000/user/elisacatena/input/historical_stock_prices1.csv"
stock_info_path = "hdfs://localhost:9000/user/elisacatena/input/historical_stocks1.csv"

# Caricamento dei dati da HDFS
stock_prices = spark.read.csv(stock_prices_path, header=True, inferSchema=True)
stock_info = spark.read.csv(stock_info_path, header=True, inferSchema=True)

# Creazione di tabelle temporanee
stock_prices.createOrReplaceTempView("stock_prices")
stock_info.createOrReplaceTempView("stock_info")

# Esecuzione di query SQL per calcolare le statistiche
statistics_query = """
WITH stats AS (
    SELECT
        sp.ticker AS Ticker,
        si.name AS Name,
        YEAR(sp.date) AS Year,
        FIRST_VALUE(sp.close) OVER (PARTITION BY sp.ticker, YEAR(sp.date) ORDER BY sp.date) AS first_close,
        LAST_VALUE(sp.close) OVER (PARTITION BY sp.ticker, YEAR(sp.date) ORDER BY sp.date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_close,
        MIN(sp.low) OVER (PARTITION BY sp.ticker, YEAR(sp.date)) AS min_price,
        MAX(sp.high) OVER (PARTITION BY sp.ticker, YEAR(sp.date)) AS max_price,
        AVG(sp.volume) OVER (PARTITION BY sp.ticker, YEAR(sp.date)) AS avg_volume
    FROM
        stock_prices sp
    JOIN
        stock_info si
    ON
        sp.ticker = si.ticker
)
SELECT
    Ticker,
    Name,
    Year,
    ROUND((last_close - first_close) / first_close * 100, 2) AS `Percent Change`,
    ROUND(min_price, 2) AS `Min Price`,
    ROUND(max_price, 2) AS `Max Price`,
    ROUND(avg_volume, 2) AS `Avg Volume`
FROM
    stats
GROUP BY
    Ticker, Name, Year, first_close, last_close, min_price, max_price, avg_volume
ORDER BY
    Ticker, Year
"""

# Esegui la query SQL e ottieni il risultato
statistics = spark.sql(statistics_query)

# Salvataggio del risultato su HDFS
output_path = "hdfs://localhost:9000/user/elisacatena/output/Spark/job1/stock_statistics_SQL.csv"
statistics.write.csv(output_path, header=True)

# Arresto della sessione Spark
spark.stop()
