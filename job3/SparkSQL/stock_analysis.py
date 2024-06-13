#!/usr/bin/env python3
"""spark application"""

import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, first, last, round, concat_ws, collect_list, lit
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
df = data.filter(col("year") >= 2000)

# Calcolo delle statistiche annuali per ogni azione
window = Window.partitionBy("ticker", "year").orderBy("date")
statistics = df.withColumn("first_close", first("close").over(window)) \
    .withColumn("last_close", last("close").over(window)) \
    .groupBy("ticker", "year") \
    .agg(
        round(((last("close") - first("close")) / first("close") * 100), 2).alias("Percent Change"),
    ) \
    .orderBy("ticker", "year")

# Rinomina le colonne con lettere maiuscole
statistics = statistics.withColumnRenamed("ticker", "Ticker") \
    .withColumnRenamed("year", "Year")

# Calcolo della finestra mobile di 3 anni
window_spec = Window.partitionBy("Ticker").orderBy("Year").rowsBetween(0, 2)

# Creazione delle colonne per gli anni e le variazioni percentuali raggruppati
grouped_data = statistics.withColumn("Years_Group", collect_list("Year").over(window_spec)) \
                         .withColumn("Percent_Changes_Group", collect_list("Percent Change").over(window_spec)) \
                         .filter(col("Years_Group").getItem(2).isNotNull()) \
                         .withColumn("Years", concat_ws(", ", col("Years_Group"))) \
                         .withColumn("Percent Changes", concat_ws(", ", col("Percent_Changes_Group")))

# Selezione e ordine delle colonne finali
final_result = grouped_data.select("Ticker", "Years", "Percent Changes") \
                           .orderBy("Ticker", "Years")

# Salvataggio del risultato su HDFS come file CSV
final_result.coalesce(1).write.csv(output_path, header=True, mode="overwrite")

# Arresto della sessione Spark
spark.stop()