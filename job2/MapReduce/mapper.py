#!/usr/bin/env python3
"""mapper.py"""

import csv
import sys
from datetime import datetime

# Mapper
def mapper():
    stocks = {}
    
    for line in sys.stdin:
        parts = list(csv.reader([line]))[0]
        if parts[0] == 'ticker':
            continue
        # Se la riga contiene i valori di historical_stocks
        if len(parts) == 5:
            ticker, _, _, sector, industry = parts
            stocks[ticker] = (sector, industry)
        # Se la riga contiene i valori di historical_stock_prices
        elif len(parts) == 8:
            ticker, _, close, _, _, _, volume, date = parts
            close = float(close)
            volume = int(volume)
            year = datetime.strptime(date, "%Y-%m-%d").year
            if ticker in stocks:
                sector, industry = stocks[ticker]
                # Emetti la coppia chiave-valore
                print(f"{sector:<20}\t{industry:<30}\t{year:<4}\t{date:<10}\t{ticker:<4}\t{close:<4}\t{volume:<4}")
        else:
            # Trova la posizione della parola "Ticker"
            ticker_index = line.find('ticker')
            new_line = line[:ticker_index]
            parts = list(csv.reader([new_line]))[0]
            ticker, _, _, sector, industry = parts
            stocks[ticker] = (sector, industry)

if __name__ == "__main__":
    mapper()


