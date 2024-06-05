import pandas as pd

# df_1 = pd.read_csv("/Users/elisacatena/Desktop/historical_stocks/historical_stock_prices.csv")
# df_2 = pd.read_csv("/Users/elisacatena/Desktop/historical_stocks/historical_stocks.csv")

df_1 = pd.read_csv("/Users/elisacatena/Desktop/historical_stock_prices.csv")
df_2 = pd.read_csv("/Users/elisacatena/Desktop/historical_stocks.csv")

merged_data = pd.merge(df_1, df_2, on='ticker')


merged_data.to_csv("/Users/elisacatena/Desktop/merged_data.csv", index=False)
