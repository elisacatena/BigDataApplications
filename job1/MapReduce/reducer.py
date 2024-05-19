#!/usr/bin/env python3
"""reducer.py"""

import sys
from collections import defaultdict
from datetime import datetime

def read_mapper_output(file):
    for line in file:
        yield line.strip().split("\t")

def main():
    all_data = defaultdict(lambda: defaultdict(lambda: {
        'dates': [],
        'close_prices': {},
        'min_price': float('inf'),
        'max_price': float('-inf'),
        'total_volume': 0,
        'count': 0
    }))

    for tokens in read_mapper_output(sys.stdin):
        ticker, year, date, close_price, low_price, high_price, volume = tokens
        year = int(year)
        date = datetime.strptime(date, "%Y-%m-%d")
        close_price = float(close_price)
        low_price = float(low_price)
        high_price = float(high_price)
        volume = int(volume)
        
        data = all_data[ticker][year]
        data['dates'].append(date)
        data['close_prices'][date] = close_price
        data['min_price'] = min(data['min_price'], low_price)
        data['max_price'] = max(data['max_price'], high_price)
        data['total_volume'] += volume
        data['count'] += 1

    for ticker, years_data in all_data.items():
        for year, data in years_data.items():
            print_statistics(ticker, year, data)

def print_statistics(ticker, year, data):
    sorted_dates = sorted(data['dates'])
    first_date = sorted_dates[0]
    last_date = sorted_dates[-1]
    first_close = data['close_prices'][first_date]
    last_close = data['close_prices'][last_date]
    min_price = data['min_price']
    max_price = data['max_price']
    avg_volume = data['total_volume'] / data['count']
    percent_change = ((last_close - first_close) / first_close) * 100
    print(f"{ticker:<10}\t{year:<4}\t{percent_change:<16}\t{min_price:<16}\t{max_price:<16}\t{avg_volume:<16}")    
    
if __name__ == "__main__":
    main()
