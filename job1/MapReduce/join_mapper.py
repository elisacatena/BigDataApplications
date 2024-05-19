#!/usr/bin/env python3
"""join_mapper.py"""

import sys
import csv 

def read_input(file):
    for line in file:
        yield line

def main():
    for line in sys.stdin:
        # Usa il modulo csv per leggere la riga correttamente
        parts = list(csv.reader([line]))[0]
        if parts[0] == 'ticker':
            continue
        if len(parts) == 5:
            if line[0] == 'ticker':
                continue  # skip header
            # This is a line from historical_stocks
            # Estrai i valori dei campi
            ticker, exchange, name, sector, industry = parts
            # Stampa l'output nel formato desiderato
            print(f"{ticker}\tstock\t{name}")
        else:
            # This is a line from stock_statistics
            parts = line.strip().split("\t")
            if len(parts) >= 6:
                ticker = parts[0]
                rest_of_line = "\t".join(parts[1:])
                print(f"{ticker}\tstat\t{rest_of_line}")

if __name__ == "__main__":
    main()
