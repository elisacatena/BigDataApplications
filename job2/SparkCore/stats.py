#!/usr/bin/env python3
"""spark application"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import year, max as spark_max, col
from pyspark.sql.window import Window
from pyspark.sql.functions import rank

# Inizializza SparkSession
spark = SparkSession.builder \
    .appName("VolumeMaxTransactionsReport") \
    .getOrCreate()

# Percorsi dei file su HDFS
stock_prices_path = "hdfs://localhost:9000/user/elisacatena/input/historical_stock_prices1.csv"
stock_info_path = "hdfs://localhost:9000/user/elisacatena/input/historical_stocks1.csv"

# Caricamento dei dati da HDFS
historical_stock_prices = spark.read.csv(stock_prices_path, header=True, inferSchema=True)
historical_stocks = spark.read.csv(stock_info_path, header=True, inferSchema=True)

# Unione dei dati
merged_df = historical_stock_prices.join(historical_stocks, "ticker")

# Aggregazione dei dati per industria, anno e ticker
industry_year_ticker_volume_df = merged_df \
    .withColumn("year", year(col("date"))) \
    .groupBy("industry", "year", "ticker") \
    .agg({"volume": "sum"}) \
    .withColumnRenamed("sum(volume)", "total_volume")

# Identificazione dell'azione con il massimo volume di transazioni per industria e anno
max_volume_df = industry_year_ticker_volume_df \
    .groupBy("industry", "year") \
    .agg(spark_max("total_volume").alias("max_volume"))

statistics = industry_year_ticker_volume_df \
    .join(max_volume_df, ["industry", "year"]) \
    .select("year", "industry", "ticker", "total_volume") \
    .orderBy("industry")

# Definisci una finestra partizionata per industria e anno, ordinata per volume decrescente
window_spec = Window.partitionBy("industry", "year").orderBy(col("total_volume").desc())

# Aggiungi una colonna di rango basata sul volume nella finestra definita sopra
ranked_statistics = statistics.withColumn("rank", rank().over(window_spec))

# Filtra solo le righe con rango 1, che corrispondono al ticker con il volume massimo per ogni industria e anno
max_volume_statistics = ranked_statistics.filter(col("rank") == 1).drop("rank")

# Creazione del report
output_path = "hdfs://localhost:9000/user/elisacatena/output/Spark/job2/stock_statistics.csv"
max_volume_statistics.coalesce(1).write.mode("overwrite").csv(output_path, header=True)

# Termina SparkSession
spark.stop()

