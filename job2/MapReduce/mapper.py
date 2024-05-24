#!/usr/bin/env python3
"""mapper.py"""

import csv
import sys
from datetime import datetime

def read_input(file):
    for line in csv.reader(file):
        yield line
        
# Mapper
def mapper():
    
    for row in read_input(sys.stdin):
        if row[0] == 'ticker':
            continue
        ticker = row[0]
        name = row[9]
        date = row[7]
        year = datetime.strptime(date, "%Y-%m-%d").year
        volume = int(row[6])
        sector = row[10]
        industry = row[11]
        close = row[2]
        print(f"{sector:<20}\t{industry:<30}\t{year:<4}\t{date:<10}\t{ticker:<4}\t{close:<4}\t{volume:<4}")

if __name__ == "__main__":
    mapper()


