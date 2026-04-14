import os
import yfinance as yf
import pandas as pd

os.makedirs("data", exist_ok=True)

df = yf.download("AAPL", period="1d", interval="5m", progress=False)

if df.empty:
    print("No data returned from yfinance.")
else:
    df.reset_index(inplace=True)
    df.to_csv("data/test.csv", index=False)
    print("Data updated!")
    print(df.tail())