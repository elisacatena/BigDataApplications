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
    volume = int(fields[6])
    sector = fields[10]
    industry = fields[11]
    return ((sector, industry, year, ticker), (date, close, volume))

def sort_and_calculate_stats(values):
    sorted_values = sorted(values, key=lambda x: x[0])
    _, close_prices, volumes = zip(*sorted_values)
    first_close = close_prices[0]
    last_close = close_prices[-1]
    total_volume = sum(volumes)

    percentual_variation = ((last_close - first_close) / first_close) * 100

    return (total_volume, first_close, last_close, percentual_variation)

def calculate_overall_stats(key, values):
    sector, industry, year = key
    tickers = {}
    for value in values:
        ticker, stats = value
        if ticker not in tickers:
            tickers[ticker] = []
        tickers[ticker].append(stats)
    
    max_volume = 0
    max_ticker_volume = None
    max_increase = -float('inf')
    max_ticker_increase = None
    industry_first_close_sum = 0
    industry_last_close_sum = 0

    for ticker, ticker_values in tickers.items():
        sorted_stats = sort_and_calculate_stats(ticker_values)
        total_volume, first_close, last_close, percentual_variation = sorted_stats

        industry_first_close_sum += first_close
        industry_last_close_sum += last_close

        if total_volume > max_volume:
            max_volume = total_volume
            max_ticker_volume = ticker

        if percentual_variation > max_increase:
            max_increase = percentual_variation
            max_ticker_increase = ticker

    industry_price_change = ((industry_last_close_sum - industry_first_close_sum) / industry_first_close_sum) * 100
    return (sector, year, industry, round(industry_price_change, 2), max_ticker_increase, round(max_increase, 2), max_ticker_volume, max_volume)

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
data = lines.map(parse_line).filter(lambda x: x is not None)

grouped_data = data.map(lambda x: ((x[0][0], x[0][1], x[0][2]), (x[0][3], (x[1][0], x[1][1], x[1][2])))) \
                   .groupByKey() \
                   .mapValues(list)

results = grouped_data.map(lambda x: calculate_overall_stats(x[0], x[1]))

output = results.sortBy(lambda x: (x[0], -x[3]))

def format_output(row):
    sector, year, industry, industry_price_change, max_ticker_increase, max_increase, max_ticker_volume, max_volume = row
    return f'"{sector}",{year},"{industry}",{industry_price_change},{max_ticker_increase} ({max_increase}),{max_ticker_volume} ({max_volume})'

formatted_output = output.map(format_output)

header = "Sector, Year, Industry, Industry price change %, Max increase ticker (increase %), Max volume ticker (volume)"
formatted_output_with_header = spark.sparkContext.parallelize([header]) \
    .union(formatted_output)

formatted_output_with_header.coalesce(1).saveAsTextFile(output_filepath)

spark.stop()