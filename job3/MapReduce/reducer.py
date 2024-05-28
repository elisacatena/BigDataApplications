#!/usr/bin/env python3
"""reducer.py"""

import itertools
import sys
from collections import defaultdict
from datetime import datetime

def read_input(file):
    for line in file:
        yield line.strip().split('\t')

def calculate_annual_trend(prices):
    trends = {}
    for year in prices:
        start_price = prices[year][0][1]  # Extract the close_price of the first element in the list
        end_price = prices[year][-1][1]   # Extract the close_price of the last element in the list
        trend = ((end_price - start_price) / start_price) * 100
        trends[year] = round(trend,2)
    return trends

def find_tickers_with_same_trend(data):
    result = []
    
    for ticker, trends in data.items():
        print(f"Ticker: {ticker}, Trends: {trends}")
        consecutive_years = 1
        prev_trend = None
        
        for year, trend in sorted(trends.items()):
            print(f"\tYear: {year}, Trend: {trend}")
            if prev_trend is None:
                prev_trend = trend
            elif trend == prev_trend:
                consecutive_years += 1
            else:
                consecutive_years = 1
                prev_trend = trend
        
        if consecutive_years >= 3:
            print(f"Trend found for {ticker}: {prev_trend} for at least three consecutive years")
            result.append(ticker)    
    return result

def main():
    data = defaultdict(lambda: defaultdict(list))
    
    # Read the mapper output
    for line in read_input(sys.stdin):
        ticker, name, year, date, close_price = line
        year = int(year)
        close_price = float(close_price)
        data[ticker][year].append((date, close_price))
    
    # Sort prices by date
    for ticker, years in data.items():
        for year, prices in years.items():
            data[ticker][year] = sorted(prices, key=lambda x: datetime.strptime(x[0], "%Y-%m-%d"))
    
    # Calculate annual trends
    for ticker, years in data.items():
        trends = calculate_annual_trend(years)
        data[ticker] = trends
    
    # Find tickers with the same trend for at least three consecutive years
    tickers_with_same_trend = find_tickers_with_same_trend(data)

    # Print the tickers with the same trend
    print("Tickers with the same trend for at least three consecutive years:")
    for ticker in tickers_with_same_trend:
        print(ticker)
    

if __name__ == "__main__":
    main()
