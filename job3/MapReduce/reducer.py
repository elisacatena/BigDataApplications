#!/usr/bin/env python3
"""reducer.py"""

import sys
from collections import defaultdict

def read_mapper_output(file):
    for line in file:
        yield line.strip().split('\t')

def main():

    # A dictionary to store prices for each ticker and year
    ticker_data = defaultdict(lambda: defaultdict(list))

    for tokens in read_mapper_output(sys.stdin):
        ticker, name, year, date, close_price = tokens
        year = int(year)
        close_price = float(close_price)
        if year >= 2000:
            ticker_data[ticker][year].append((date, close_price))

    for ticker, years in ticker_data.items():
        sorted_years = sorted(years.keys())
        for i in range(len(sorted_years) - 2):
            three_years = sorted_years[i:i+3]
            percent_changes = []
            for year in three_years:
                # Sort by date
                price_data = sorted(years[year])
                first_date, first_close = price_data[0]
                last_date, last_close = price_data[-1]
                percent_change = ((last_close - first_close) / first_close) * 100
                percent_changes.append(round(percent_change,2))
            print(f"{ticker} {three_years} {percent_changes}")

if __name__ == "__main__":
    main()
