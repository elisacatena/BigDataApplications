# Analisi Storica Giornaliera dei Prezzi delle Azioni

Questo progetto si concentra sull'analisi dei dati storici giornalieri dei prezzi delle azioni dalla Borsa di New York (NYSE) e dal NASDAQ, che spaziano dal 1970 al 2018. Il dataset è composto da due file CSV:

1. `historical_stock_prices.csv`: Contiene informazioni giornaliere sui prezzi delle azioni, inclusi i prezzi di apertura, chiusura, minimi, massimi, il volume e la data.
2. `historical_stocks.csv`: Contiene dettagli su ciascuna azione, inclusi il simbolo, la borsa, il nome dell'azienda, il settore e l'industria.

## Preparazione dei Dati
- Pulizia dei dati mediante rimozione di voci errate o non pertinenti.
- Gestione dei valori mancanti, se presenti.
- Garanzia di coerenza e correttezza dei dati.

## Applicazioni Implementate
1. **Statistiche sulle Prestazioni delle Azioni per Anno**
   - Generazione di statistiche per ogni azione, incluse il simbolo, il nome dell'azienda e le metriche di prestazione annuale come la variazione percentuale, il prezzo minimo, il prezzo massimo e il volume medio.
   - Implementato in MapReduce, Hive, Spark Core e Spark SQL.

2. **Analisi per Settore Industriale**
   - Produzione di un rapporto dettagliato sulla variazione percentuale, l'azione più performante e l'azione con il volume di transazioni più alto per ogni industria annualmente.
   - Le industrie sono raggruppate per settore e ordinate per variazione percentuale.
   - Implementato in MapReduce, Hive, Spark Core e Spark SQL.

3. **Identificazione di Aziende con Trend Simili**
   - Identificazione di gruppi di aziende le cui azioni hanno mostrato trend annuali simili per almeno tre anni consecutivi a partire dal 2000.
   - Presentazione delle aziende insieme al loro trend comune per ogni anno.
   - Implementato in MapReduce, Hive, Spark Core e Spark SQL.

## Contenuti del Rapporto
Per ciascuna applicazione, il rapporto finale include:
- Passaggi di preparazione dei dati.
- Pseudocodice per le implementazioni MapReduce e Spark Core.
- Le prime 10 righe dei risultati.
- Tabella di confronto e grafici che illustrano i tempi di esecuzione su ambiente locale e cluster con dimensioni di input crescenti.
- Collegamenti ai repository completi del codice per le implementazioni MapReduce e Spark.

Per ulteriori dettagli, fare riferimento alla documentazione individuale fornita nei rispettivi repository.

