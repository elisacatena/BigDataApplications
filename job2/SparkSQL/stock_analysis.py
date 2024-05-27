#!/usr/bin/env python3
"""spark application"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list, year, first, last, sum as spark_sum, max as spark_max, round, row_number
from pyspark.sql.window import Window

def stock_analysis():
    spark = SparkSession.builder \
        .appName("Stock Statistics") \
        .getOrCreate()

    # Percorsi dei file su HDFS
    input_path = "hdfs:///user/elisacatena/input/merged_data.csv"
    output_path = "hdfs:///user/elisacatena/output/Spark/job2/stock_statistics_SQL"

    # Caricamento dei dati da HDFS
    data = spark.read.csv(input_path, header=True, inferSchema=True)

    # Preprocessing
    data = data.withColumn("year", year(col("date")))

    # Raggruppa i dati per settore, industria e anno
    grouped_data = data.groupBy("sector", "industry", "year", "ticker") \
                              .agg(
                                  collect_list("close").alias("close_prices"),
                                  spark_sum("volume").alias("total_volume"),
                                  first("close").alias("first_close"),
                                  last("close").alias("last_close")
                              )

    # Calcola le statistiche
    grouped_data = grouped_data.withColumn("percent_increase", (col("last_close") - col("first_close")) / col("first_close") * 100)

    # Raggruppa di nuovo per settore, industria e anno per calcolare i valori massimi
    final_data = grouped_data.groupBy("sector", "industry", "year") \
                             .agg(
                                 spark_sum("first_close").alias("industry_first_close_sum"),
                                 spark_sum("last_close").alias("industry_last_close_sum"),
                                 spark_max("percent_increase").alias("max_percent_increase"),
                                 spark_max("total_volume").alias("max_total_volume")
                             )

    # Calcola la variazione percentuale della quotazione dell'industria
    final_data = final_data.withColumn("Industry price change %", round(((col("industry_last_close_sum") - col("industry_first_close_sum")) / col("industry_first_close_sum") * 100), 2))

    # Finestra per trovare il ticker con il massimo incremento percentuale
    window_spec_increase = Window.partitionBy("sector", "industry", "year").orderBy(col("percent_increase").desc())
    grouped_data = grouped_data.withColumn("rank_increase", row_number().over(window_spec_increase))
    max_increase_ticker = grouped_data.filter(col("rank_increase") == 1).select("sector", "industry", "year", col("ticker").alias("Max increase ticker"), round(col("percent_increase"),2).alias("Max increase %"))

    # Finestra per trovare il ticker con il volume massimo
    window_spec_volume = Window.partitionBy("sector", "industry", "year").orderBy(col("total_volume").desc())
    grouped_data = grouped_data.withColumn("rank_volume", row_number().over(window_spec_volume))
    max_volume_ticker = grouped_data.filter(col("rank_volume") == 1).select("sector", "industry", "year", col("ticker").alias("Max volume ticker"), col("total_volume").alias("Max volume"))

    # Unisci i dati finali con i ticker massimo incremento e volume
    final_data = final_data.join(max_increase_ticker, on=["sector", "industry", "year"], how="left")
    final_data = final_data.join(max_volume_ticker, on=["sector", "industry", "year"], how="left")

    # Seleziona le colonne finali e riformatta i dati per la scrittura
    final_data = final_data.select(
        col("sector").alias("Sector"),
        col("year").alias("Year"),
        col("industry").alias("Industry"),
        col("Industry price change %"),
        col("Max increase ticker"),
        col("Max increase %"),
        col("Max volume ticker"),
        col("Max volume")
    )

    # Ordina i dati per settore e variazione percentuale decrescente
    final_data = final_data.orderBy(col("Sector"), col("Industry price change %").desc())

    # Scrivi i risultati su HDFS in formato CSV
    final_data.coalesce(1).write.csv(output_path, header=True, mode="overwrite")
    
    spark.stop()

if __name__ == "__main__":
    stock_analysis()
