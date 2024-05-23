#!/usr/bin/env python3
"""spark application"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, min, max, avg, first, last, round
from pyspark.sql import Window

# Creazione della sessione Spark
spark = SparkSession.builder \
    .appName("Stock Statistics") \
    .getOrCreate()

# Percorsi dei file su HDFS
stock_prices_path = "hdfs://localhost:9000/user/elisacatena/input/historical_stock_prices1.csv"
stock_info_path = "hdfs://localhost:9000/user/elisacatena/input/historical_stocks1.csv"

# Caricamento dei dati da HDFS
stock_prices = spark.read.csv(stock_prices_path, header=True, inferSchema=True)
stock_info = spark.read.csv(stock_info_path, header=True, inferSchema=True)

# Preprocessing
stock_prices = stock_prices.withColumn("year", year(col("date")))

# Unione dei dati
data = stock_prices.join(stock_info, on="ticker")

# Calcolo delle statistiche annuali per ogni azione
window_spec = Window.partitionBy("ticker", "year").orderBy("date")

statistics = data.withColumn("first_close", first("close").over(window_spec)) \
    .withColumn("last_close", last("close").over(window_spec)) \
    .groupBy("ticker", "name", "year") \
    .agg(
        round(((last("close") - first("close")) / first("close") * 100), 2).alias("Percent Change"),
        round(min("low"), 2).alias("Min Price"),
        round(max("high"), 2).alias("Max Price"),
        round(avg("volume"), 2).alias("Avg Volume")
    ) \
    .orderBy("ticker", "year")
    
# Rinomina le colonne con lettere maiuscole
statistics = statistics.withColumnRenamed("ticker", "Ticker") \
                       .withColumnRenamed("name", "Name") \
                       .withColumnRenamed("year", "Year")

# Salvataggio del risultato su HDFS
output_path = "hdfs://localhost:9000/user/elisacatena/output/Spark/job1/stock_statistics.csv"
statistics.write.csv(output_path, header=True)

# Arresto della sessione Spark
spark.stop()
