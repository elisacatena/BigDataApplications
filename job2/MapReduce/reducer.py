#!/usr/bin/env python3
"""reducer.py"""

import sys
from collections import defaultdict

def read_mapper_output(file):
    for line in file:
        yield line.strip().split("\t")
        
def reducer():
    
    print("{:<30}\t{:<4}\t{:<30}\t{:<35}\t{:<35}\t{:<30}".format("Sector", "Year", "Industry", "Industry price change %", "Max increase ticker (increase %)", "Max volume ticker (volume)"))
    
    # Dati strutturati come: { (sector, industry, year): { ticker: [total_volume, [close_prices]] } }
    industry_year_data = defaultdict(lambda: defaultdict(lambda: [0, []]))

    for tokens in read_mapper_output(sys.stdin):

        sector, industry, year, date, ticker, close_price, volume = tokens
        close_price = float(close_price)
        volume = int(volume)
        year = int(year)

        # Aggiorna i dati per volume e prezzi di chiusura
        ticker_data = industry_year_data[(sector, industry, year)][ticker]
        ticker_data[0] += volume  # Accumula il volume
        ticker_data[1].append((date, close_price))  # Aggiungi data e prezzo di chiusura

    # Lista per memorizzare i risultati
    results = []

    # Ora dobbiamo trovare il ticker con il volume massimo e con il maggior incremento percentuale
    for (sector, industry, year), ticker_data in industry_year_data.items():
        max_volume = 0
        max_ticker_volume = None
        max_increase = -float('inf')
        max_ticker_increase = None
        industry_first_close_sum = 0
        industry_last_close_sum = 0

        for ticker, data in ticker_data.items():
            total_volume, close_prices = data

            # Ordina i prezzi di chiusura per data
            close_prices.sort(key=lambda x: x[0])

            # Trova il primo e l'ultimo prezzo di chiusura dell'anno
            first_close = close_prices[0][1]
            last_close = close_prices[-1][1]

            # Aggiungi i prezzi di chiusura alla somma dell'industria
            industry_first_close_sum += first_close
            industry_last_close_sum += last_close

            # Calcolo incremento percentuale
            percent_increase = ((last_close - first_close) / first_close) * 100

            # Se il volume è massimo, aggiorna il massimo volume e il ticker corrispondente
            if total_volume > max_volume:
                max_volume = total_volume
                max_ticker_volume = ticker

            # Se l'incremento percentuale è massimo, aggiorna il massimo incremento e il ticker corrispondente
            if percent_increase > max_increase:
                max_increase = percent_increase
                max_ticker_increase = ticker

        # Calcola la variazione percentuale della quotazione dell'industria
        industry_price_change = ((industry_last_close_sum - industry_first_close_sum) / industry_first_close_sum) * 100

        # Aggiungi i risultati alla lista
        results.append((sector, year, industry, max_increase, max_ticker_increase, max_volume, max_ticker_volume, industry_price_change))

    # Ordina i risultati per settore e variazione percentuale decrescente
    results.sort(key=lambda x: (x[0], -x[3]))

    # Stampa i risultati
    for sector, year, industry, max_increase, max_ticker_increase, max_volume, max_ticker_volume, industry_price_change in results:
        max_increase_output = f"{max_ticker_increase} ({max_increase:.2f})"
        max_volume_output = f"{max_ticker_volume} ({max_volume})"
        industry_price_change_output = f"{industry_price_change:.2f}"

        print("{:<30}\t{:<4}\t{:<30}\t{:<35}\t{:<35}\t{:<30}".format(sector, year, industry, industry_price_change_output, max_increase_output, max_volume_output))

if __name__ == "__main__":
    reducer()

