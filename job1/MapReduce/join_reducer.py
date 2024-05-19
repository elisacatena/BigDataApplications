#!/usr/bin/env python3
"""join_reducer.py"""

import sys
from collections import defaultdict

def read_mapper_output(file):
    for line in file:
        yield line.strip().split("\t")

def main():
    stocks_data = {}
    statistics_data = defaultdict(list)
    
    # Stampa l'intestazione
    print("{:<10}\t{:<50}\t{:<4}\t{:<16}\t{:<16}\t{:<16}\t{:<16}".format("Ticker", "Name", "Year", "Percent Change", "Min Price", "Max Price", "Avg Volume"))
    
    for tokens in read_mapper_output(sys.stdin):
        if tokens[1] == 'stock':
            ticker = tokens[0].strip()
            name = tokens[2].strip()
            stocks_data[ticker] = name
        else:
            ticker = tokens[0].strip()
            statistics_data[ticker].append(tokens[2:])
    
    for ticker, stats in statistics_data.items():
        name = stocks_data.get(ticker, "Unknown")
        for stat in stats:
            year, percent_change, min_price, max_price, avg_volume = stat
            print("{:<10}\t{:<50}\t{:<4}\t{:<16}\t{:<16}\t{:<16}\t{:<16}".format(ticker, name, year, percent_change, min_price, max_price, avg_volume))
            # print(f"{ticker}\t{name}\t{year}\t{percent_change}\t{min_price}\t{max_price}\t{avg_volume}")

if __name__ == "__main__":
    main()
