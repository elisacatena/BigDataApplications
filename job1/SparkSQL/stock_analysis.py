#!/usr/bin/env python3
"""spark application"""

import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, min, max, avg, first, last, round
from pyspark.sql import Window

# Creazione del parser e impostazione degli argomenti
parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file path")
parser.add_argument("--output_path", type=str, help="Output file path")

# Parsing degli argomenti
args = parser.parse_args()
input_path = args.input_path
output_path = args.output_path

# Inizializzazione della SparkSession con la configurazione appropriata
spark = SparkSession.builder \
    .appName("Stock Statistics") \
    .getOrCreate()

# Caricamento dei dati da HDFS
data = spark.read.csv(input_path, header=True)

# Preprocessing: aggiunta della colonna "year"
data = data.withColumn("year", year(col("date")))

# Calcolo delle statistiche annuali per ogni azione
window = Window.partitionBy("ticker", "year").orderBy("date")
statistics = data.withColumn("first_close", first("close").over(window)) \
    .withColumn("last_close", last("close").over(window)) \
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
final_result = joined_data.orderBy("Ticker", "year") \
                          .select("Ticker", "name", "year", "Percent Change", "Min Price", "Max Price", "Avg Volume")

# Rinomina le colonne con lettere maiuscole
final_result = final_result.withColumnRenamed("name", "Name") \
                           .withColumnRenamed("year", "Year")

# Salvataggio del risultato su HDFS come file CSV
final_result.coalesce(1).write.csv(output_path, header=True)

# Arresto della sessione Spark
spark.stop()