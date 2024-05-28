#!/usr/bin/env python3
"""mapper.py"""

import sys
import csv
from datetime import datetime

def read_input(file):
    for line in csv.reader(file):
        yield line

def main():
    for row in read_input(sys.stdin):
        if row[0] == 'ticker':
            continue  # skip header
        ticker = row[0]
        name = row[9]
        date = row[7]
        year = datetime.strptime(date, "%Y-%m-%d").year
        close_price = float(row[2])
        
        print(f"{ticker:<10}\t{name:<40}\t{year:<4}\t{date:<10}\t{close_price:<16}")

if __name__ == "__main__":
    main()