#!/usr/bin/env python3
"""reducer.py"""

import sys
from collections import defaultdict

def read_mapper_output(file):
    for line in file:
        yield line.strip().split('\t')

def main():

    # Dizionario per memorizzare i prezzi per ciascun ticker e anno
    ticker_data = defaultdict(lambda: defaultdict(list))
    # Dizionario per memorizzare le variazioni percentuali
    group_changes = defaultdict(lambda: defaultdict(list))

    for tokens in read_mapper_output(sys.stdin):
        ticker, year, date, close_price = tokens
        year = int(year)
        close_price = float(close_price)
        ticker_data[ticker][year].append((date, close_price))
        
    for ticker, years in ticker_data.items():
        sorted_years = sorted(years.keys())
        # Considera gruppi di tre consecutivi
        for i in range(len(sorted_years) - 2):
            three_years = sorted_years[i:i+3]
            percent_changes = []
            # Calcola la variazione percentuale per ciascun anno nel gruppo
            for year in three_years:
                price_data = sorted(years[year])
                _, first_close = price_data[0]
                _, last_close = price_data[-1]
                percent_change = ((last_close - first_close) / first_close) * 100
                percent_changes.append(round(percent_change,2))
            group_percent_changes = tuple(percent_changes)
            group_changes[group_percent_changes][ticker] = three_years

    print("{:<30}\t{:<30}\t{:<30}".format("Tickers", "Years", "Percent Changes"))
    for group_percent_changes, ticker_years in group_changes.items():
        if len(ticker_years) >= 2:
            tickers = ", ".join(str(ticker) for ticker in ticker_years.keys()).replace(" ","")
            years = ", ".join(str(year) for year in list(ticker_years.values())[0])
            percent_changes = "%, ".join(str(change) for change in group_percent_changes)
            print("{:<30}\t{:<30}\t{:<30}".format(tickers, years, percent_changes+'%'))

if __name__ == "__main__":
    main()
