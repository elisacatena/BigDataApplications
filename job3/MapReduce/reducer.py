#!/usr/bin/env python3
"""reducer.py"""

import itertools
import sys
from collections import Counter, defaultdict
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

from itertools import combinations

def find_tickers_with_same_trend(data):
    # Dizionario per memorizzare i sottoinsiemi di lunghezza 3 per ciascun ticker
    trend_subsets = {}
    
    for ticker, trends in data.items():
        trend_values = list(trends.values())  # Ottieni la lista dei trend per il ticker corrente
        subsets = set(combinations(trend_values, 3))  # Trova tutti i sottoinsiemi di lunghezza 3
        trend_subsets[ticker] = subsets
    
    # Conta quanti ticker condividono almeno uno dei sottoinsiemi
    common_subsets = Counter()
    for subsets in trend_subsets.values():
        common_subsets.update(subsets)
    
    # Trova i ticker che condividono almeno uno dei sottoinsiemi in comune
    result = {}
    for ticker, subsets in trend_subsets.items():
        for subset in subsets:
            if common_subsets[subset] >= 2:
                if subset not in result:
                    result[subset] = [ticker]
                else:
                    result[subset].append(ticker)
    
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
    for subset, tickers in tickers_with_same_trend.items():
        trend_str = ", ".join([f"{year}:{trend}%" for year, trend in zip(data[tickers[0]].keys(), subset)])
        print("{" + ", ".join(ticker.strip() for ticker in tickers) + "}:" + f"({trend_str})")



    

if __name__ == "__main__":
    main()
