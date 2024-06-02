import csv
import random
import string
from datetime import datetime, timedelta

riga = ["PIH", 10, 11, 8.49315452575684, 3, 14, 10, "2013-05-08", "NYSE", "\"1347 PROPERTY INSURANCE HOLDINGS, INC.\"", "FINANCE", "PROPERTY-CASUALTY INSURERS"]

# Funzione per generare un valore casuale per low, high, open, close e volume
def genera_valore():
    return round(random.uniform(1, 100), 2)

# Funzione per generare un ticker casuale
def genera_ticker():
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=3))

# Funzione per generare date consecutive
def genera_date_iniziali(inizio, num_date):
    date_iniziali = [inizio]
    for _ in range(1, num_date):
        inizio += timedelta(days=1)
        date_iniziali.append(inizio.strftime("%Y-%m-%d"))
    return date_iniziali

# Funzione per duplicare il file CSV originale con un dato fattore di scala e modificare il ticker e i valori di low, high, open, close e volume
def aggiungi_nuove_righe(file_originale, fattore_scala):
    with open(file_originale, "a", newline='') as f_originale:
        scrittore = csv.writer(f_originale)
        date_inizio = datetime.strptime(riga[7], "%Y-%m-%d")
        date_consecutive = genera_date_iniziali(date_inizio + timedelta(days=1), 2000)
        # Aggiungi nuove righe con ticker differenti e valori modificati per low, high, open, close e volume
        contatore_nuove_righe = 0
        i = 0
        j = 1
        while i < fattore_scala:
            nuovo_ticker = genera_ticker()
            ticker_name = f"Company {j}"  
            industry = f"Industry {j}"  
            j += 1
            for data in date_consecutive:
                i += 1
                nuova_riga = riga[:]  # Copia la struttura della riga originale
                nuova_riga[0] = nuovo_ticker  # Modifica il ticker con uno nuovo
                nuova_riga[1] = genera_valore()  # Modifica il valore di open
                nuova_riga[2] = genera_valore()  # Modifica il valore di close
                nuova_riga[4] = genera_valore()  # Modifica il valore di low
                nuova_riga[5] = genera_valore()  # Modifica il valore di high
                nuova_riga[6] = random.randint(100000, 300000)  # Modifica il valore di volume
                nuova_riga[7] = data  # Modifica la data
                nuova_riga[9] = ticker_name  # Modifica il nome della compagnia
                nuova_riga[11] = industry  # Modifica l'industria
                scrittore.writerow(nuova_riga)
                contatore_nuove_righe += 1
                if contatore_nuove_righe >= fattore_scala:
                    break

# File originale CSV
file_originale_1 = "merged_data_1.csv"
file_originale_2 = "merged_data_2.csv"
file_originale_3 = "merged_data_3.csv"

# Primo nuovo file (dimensione maggiore di 1/3 rispetto al file originale)
fattore_scala_1 = len(open(file_originale_1).readlines()) // 3
aggiungi_nuove_righe(file_originale_1, fattore_scala_1)
print("1")

# Secondo nuovo file (dimensione maggiore di 2/3 rispetto al file originale)
fattore_scala_2 = len(open(file_originale_2).readlines()) * 2 // 3
aggiungi_nuove_righe(file_originale_2, fattore_scala_2)
print("2")

fattore_scala_3 = len(open(file_originale_3).readlines())
aggiungi_nuove_righe(file_originale_3, fattore_scala_3)
print("3")