#!/usr/bin/env python3
"""join_mapper.py"""

import sys
import csv 

def main():
    for line in sys.stdin:
        # print(line)
        # Usa il modulo csv per leggere la riga correttamente
        parts = list(csv.reader([line]))[0]
        if parts[0] == 'ticker':
            continue
        if len(parts) == 5:
            # This is a line from historical_stocks.csv
            # Estrai i valori dei campi
            ticker, _, name, _, _ = parts
            # Stampa l'output nel formato desiderato
            print(f"{ticker}\tstock\t{name}")
        else:
            # This is a line from stock_statistics.txt
            parts = line.strip().split("\t")
            if len(parts) >= 6:
                ticker = parts[0]
                rest_of_line = "\t".join(parts[1:])
                print(f"{ticker}\tstat\t{rest_of_line}")

if __name__ == "__main__":
    main()
