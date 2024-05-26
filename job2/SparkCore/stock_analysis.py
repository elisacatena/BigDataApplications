#!/usr/bin/env python3
"""spark application"""

from datetime import datetime
import argparse
from pyspark.sql import SparkSession
import csv
from io import StringIO

def parse_line(line):
    reader = csv.reader(StringIO(line))
    fields = next(reader)
    if fields[0] == "ticker":
        return None
    ticker = fields[0]
    date = datetime.strptime(fields[7], '%Y-%m-%d')
    year = date.year
    close = float(fields[2])
    volume = float(fields[6])
    sector = fields[10]
    industry = fields[11]
    return ((sector, industry, year, ticker), (date, close, volume))

def sort_and_calculate_stats(values):
    sorted_values = sorted(values, key=lambda x: x[0])
    dates, close_prices, volumes = zip(*sorted_values)
    first_close = close_prices[0]
    last_close = close_prices[-1]
    total_volume = sum(volumes)

    # Calcolo della variazione percentuale
    percentual_variation = ((last_close - first_close) / first_close) * 100

    return (total_volume, first_close, last_close, percentual_variation)

def calculate_overall_stats(data):
    industry_data = {}

    for key, values in data.items():
        sector, industry, year, ticker = key
        if (sector, industry, year) not in industry_data:
            industry_data[(sector, industry, year)] = {}
        industry_data[(sector, industry, year)][ticker] = sort_and_calculate_stats(values)

    results = []
    for (sector, industry, year), tickers in industry_data.items():
        max_volume = 0
        max_ticker_volume = None
        max_increase = -float('inf')
        max_ticker_increase = None
        industry_first_close_sum = 0
        industry_last_close_sum = 0

        for ticker, (total_volume, first_close, last_close, percentual_variation) in tickers.items():
            industry_first_close_sum += first_close
            industry_last_close_sum += last_close

            if total_volume > max_volume:
                max_volume = total_volume
                max_ticker_volume = ticker

            if percentual_variation > max_increase:
                max_increase = percentual_variation
                max_ticker_increase = ticker

        industry_price_change = ((industry_last_close_sum - industry_first_close_sum) / industry_first_close_sum) * 100
        results.append((sector, year, industry, round(industry_price_change, 2), max_ticker_increase, round(max_increase, 2), max_ticker_volume, round(max_volume, 2)))

    return results

parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input dataset path")
parser.add_argument("--output_path", type=str, help="Output folder path")

args = parser.parse_args()
dataset_filepath, output_filepath = args.input_path, args.output_path

spark = SparkSession \
    .builder \
    .appName("Stock Annual Trend") \
    .getOrCreate()

lines = spark.sparkContext.textFile(dataset_filepath).cache()
data = lines.map(parse_line).filter(lambda x: x is not None).groupByKey().mapValues(list).collectAsMap()

results = calculate_overall_stats(data)

# Converte i risultati in un RDD e salva
output = spark.sparkContext.parallelize(results)

# Ordina l'output in base al settore e alla variazione percentuale in modo decrescente
output_sorted = output.sortBy(lambda x: (x[0], -x[3]))

# Riduci il numero di partizioni a 1 prima di salvare l'output
output_sorted.coalesce(1).saveAsTextFile(output_filepath)
