#!/usr/bin/env python3
"""spark application"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list, year, first, last, sum as spark_sum, udf
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType

def stock_analysis():
    spark = SparkSession.builder \
        .appName("Stock Statistics") \
        .getOrCreate()

    # Percorsi dei file su HDFS
    input_path = "hdfs:///user/elisacatena/input/merged_data1.csv"
    output_path = "hdfs:///user/elisacatena/output/Spark/job1/stock_statistics1_SQL"

    # Caricamento dei dati da HDFS
    data = spark.read.csv(input_path, header=True)

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
                                 collect_list("percent_increase").alias("percent_increases"),
                                 collect_list("ticker").alias("tickers"),
                                 collect_list("total_volume").alias("volumes")
                             )

    # Calcola la variazione percentuale della quotazione dell'industria
    final_data = final_data.withColumn("industry_price_change", (col("industry_last_close_sum") - col("industry_first_close_sum")) / col("industry_first_close_sum") * 100)

    # Definisci UDF per trovare il ticker con il massimo incremento percentuale e il volume massimo
    def get_max_increase(tickers, percent_increases):
        max_increase = max(percent_increases)
        max_ticker_increase = tickers[percent_increases.index(max_increase)]
        return (max_ticker_increase, max_increase)

    def get_max_volume(tickers, volumes):
        max_volume = max(volumes)
        max_ticker_volume = tickers[volumes.index(max_volume)]
        return (max_ticker_volume, max_volume)

    get_max_increase_udf = udf(get_max_increase, StructType([
        StructField("ticker", StringType(), False),
        StructField("increase", FloatType(), False)
    ]))
    get_max_volume_udf = udf(get_max_volume, StructType([
        StructField("ticker", StringType(), False),
        StructField("volume", IntegerType(), False)
    ]))

    final_data = final_data.withColumn("max_increase_ticker", get_max_increase_udf(col("tickers"), col("percent_increases")))
    final_data = final_data.withColumn("max_volume_ticker", get_max_volume_udf(col("tickers"), col("volumes")))

    # Seleziona le colonne finali e riformatta i dati per la scrittura
    final_data = final_data.select(
        col("sector"),
        col("year"),
        col("industry"),
        col("industry_price_change").alias("industry_price_change_percentage"),
        col("max_increase_ticker.ticker").alias("max_increase_ticker"),
        col("max_increase_ticker.increase").alias("max_increase_percentage"),
        col("max_volume_ticker.ticker").alias("max_volume_ticker"),
        col("max_volume_ticker.volume").alias("max_volume")
    )

    # Scrivi i risultati su HDFS in formato CSV
    final_data.write.csv(output_path, header=True, mode="overwrite")

if __name__ == "__main__":
    stock_analysis()
