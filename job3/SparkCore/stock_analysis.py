#!/usr/bin/env python3
"""spark application"""

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
    if year >= 2000:
        return (ticker, (year, date, close))
    return None

def calculate_annual_variation(values):
    sorted_values = sorted(values, key=lambda x: x[0])
    _, close_prices = zip(*sorted_values)
    first_close = close_prices[0]
    last_close = close_prices[-1]
    percent_variation = round(((last_close - first_close) / first_close) * 100, 2)
    return percent_variation

def generate_sliding_windows(years, variations):
    results = []
    for i in range(len(years) - 2):
        year_window = years[i:i+3]
        variation_window = variations[i:i+3]
        results.append((year_window, variation_window))
    return results

# Parsing command line arguments
parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input dataset path")
parser.add_argument("--output_path", type=str, help="Output folder path")
args = parser.parse_args()
dataset_filepath, output_filepath = args.input_path, args.output_path

# Initialize Spark session
spark = SparkSession.builder.appName("Stock Annual Trend").getOrCreate()

# Load input file as RDD and cache
lines = spark.sparkContext.textFile(dataset_filepath).cache()

# Parse lines and filter invalid values
parsed_lines = lines.map(parse_line).filter(lambda x: x is not None)

# Group by ticker and year
grouped_data = parsed_lines.map(lambda x: ((x[0], x[1][0]), (x[1][1], x[1][2]))).groupByKey()

# Calculate annual variation for each ticker-year
annual_variation = grouped_data.mapValues(lambda x: calculate_annual_variation(list(x))).filter(lambda x: x[1] is not None)

# Group by ticker and sort by year
ticker_year_variation = annual_variation.map(lambda x: (x[0][0], (x[0][1], x[1]))).groupByKey()

# Generate sliding windows for each ticker
sliding_windows = ticker_year_variation.flatMapValues(lambda x: generate_sliding_windows([i[0] for i in sorted(x)], [i[1] for i in sorted(x)]))

# Format output for saving
formatted_output = sliding_windows.map(lambda x: (x[0], x[1][0], x[1][1]))

# Create a combined key of years and variations for duplicate detection
combined_key_output = formatted_output.map(lambda x: ((tuple(x[1]), tuple(x[2])), (x[0], x[1], x[2])))

# Filter out unique rows based on the combined key
filtered_output = combined_key_output.groupByKey().filter(lambda x: len(x[1]) > 1).flatMap(lambda x: x[1])

# Group by years and variations
grouped_output = filtered_output.map(lambda x: ((tuple(x[1]), tuple(x[2])), (x[0], x[1], x[2]))).groupByKey()

# Filter out unique groups
unique_groups = grouped_output.filter(lambda x: len(x[1]) > 1)

# Format the output
formatted_output = unique_groups.map(lambda x: (', '.join(sorted([item[0] for item in x[1]])), ' '.join(map(str, x[0][0])), ' '.join(map(str, x[0][1]))))

# Format the output and sort alphabetically
formatted_output_sorted = formatted_output.sortBy(lambda x: (x[0]))

# Save the sorted output
formatted_output_sorted.coalesce(1).saveAsTextFile(output_filepath)

# Stop Spark session
spark.stop()
