#!/usr/bin/env python3
"""spark application"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, min, max, avg, first, last, round
from pyspark.sql import Window

# Creazione della sessione Spark
spark = SparkSession.builder \
    .appName("Stock Statistics") \
    .getOrCreate()

# Percorso dei file su HDFS
input_path = "hdfs:///user/elisacatena/input/merged_data1.csv"
output_path = "hdfs:///user/elisacatena/output/Spark/job1/stock_statistics1_SQL"

# Caricamento dei dati da HDFS
data = spark.read.csv(input_path, header=True)

# Preprocessing
data = data.withColumn("year", year(col("date")))

# Calcolo delle statistiche annuali per ogni azione
window_spec = Window.partitionBy("ticker", "year").orderBy("date")

statistics = data.withColumn("first_close", first("close").over(window_spec)) \
    .withColumn("last_close", last("close").over(window_spec)) \
    .groupBy("ticker", "year") \
    .agg(
        round(((last("close") - first("close")) / first("close") * 100), 2).alias("Percent Change"),
        round(min("low"), 2).alias("Min Price"),
        round(max("high"), 2).alias("Max Price"),
        round(avg("volume"), 2).alias("Avg Volume")
    ) \
    .orderBy("ticker", "year")
    
# Rinomina le colonne con lettere maiuscole
statistics = statistics.withColumnRenamed("ticker", "Ticker")

# Unione con i nomi delle azioni
joined_data = statistics.join(data.select("ticker", "name").dropDuplicates(), on="ticker", how="inner")

# Ordina e seleziona le colonne
final_result = joined_data.orderBy("ticker", "year") \
                          .select("Ticker", "name", "year", "Percent Change", "Min Price", "Max Price", "Avg Volume")

# Rinomina le colonne con lettere maiuscole
final_result = final_result.withColumnRenamed("ticker", "Ticker") \
                       .withColumnRenamed("name", "Name") \
                       .withColumnRenamed("year", "Year")
                       
# Salvataggio del risultato su HDFS come file CSV
final_result.coalesce(1).write.csv(output_path, header=True)

# Arresto della sessione Spark
spark.stop()