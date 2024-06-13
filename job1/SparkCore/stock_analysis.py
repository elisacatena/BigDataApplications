#!/usr/bin/env python3
"""spark aplication"""

from datetime import datetime
import argparse
from pyspark.sql import SparkSession
import csv
from io import StringIO

def parse_line(line):
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
    percent_change = round(((last_close - first_close) / first_close) * 100, 2)
    max_high = round(max(high_prices),2)
    min_low = round(min(low_prices),2)
    mean_volume = round(sum(volumes) / len(volumes),2)
    return (name[0], percent_change, min_low, max_high, mean_volume)

 
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

yearly_stats = data.mapValues(sort_and_calculate_stats)

# Maintain key-value pair structure for sorting
output = yearly_stats.map(lambda x: (x[0], f"{x[0][0]}, \"{x[1][0]}\", {x[0][1]}, {x[1][1]}, {x[1][2]}, {x[1][3]}, {x[1][4]}"))

# Sort the output by ticker
output_sorted = output.sortByKey()

# Extract the values for saving without parentheses
formatted_output = output_sorted.map(lambda x: x[1])

# Add the header
header = spark.sparkContext.parallelize(["Ticker,Name,Year,Percent Change,Min Price,Max Price,Avg Volume"])
final_output = header.union(formatted_output)

# Reduce the number of partitions to 1 before saving the output
final_output.coalesce(1).saveAsTextFile(output_filepath)

spark.stop()