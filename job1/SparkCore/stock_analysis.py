#!/usr/bin/env python3
"""spark aplication"""

from datetime import datetime
import argparse
from pyspark.sql import SparkSession
import csv
from io import StringIO

def parse_line(line):
    # fields = line.split(',')
    # Usa la libreria csv per leggere la linea correttamente
    fields = csv.reader(StringIO(line)).__next__()
    if fields[0] == "ticker":
        return None
    ticker = fields[0]
    date = datetime.strptime(fields[7], '%Y-%m-%d')
    year = date.year
    close = float(fields[2])
    low = float(fields[4])
    high = float(fields[5])
    volume = float(fields[6])
    name = fields[9]
    return ((ticker, year), (date, close, low, high, volume, name))
 
def sort_and_calculate_stats(values):
    sorted_values = sorted(values, key=lambda x: x[0])
    _, close_prices, low_prices, high_prices, volumes, name = zip(*sorted_values)
    first_close = close_prices[0]
    last_close = close_prices[-1]
    percentual_variation_rounded = round(((last_close - first_close) / first_close) * 100, 2)
    max_high = max(high_prices)
    min_low = min(low_prices)
    mean_volume = sum(volumes) / len(volumes)
    return (name[0], percentual_variation_rounded, min_low, max_high, mean_volume)

 
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
data = lines.map(parse_line).filter(lambda x: x is not None).groupByKey()

stats_per_stock_per_year = data.mapValues(sort_and_calculate_stats)
output = stats_per_stock_per_year.map(lambda x: (x[0], x[1]))
 
# Ordina l'output in base al ticker
output_sorted = output.sortByKey()

# Riduci il numero di partizioni a 1 prima di salvare l'output
output_sorted.coalesce(1).saveAsTextFile(output_filepath)

spark.stop()