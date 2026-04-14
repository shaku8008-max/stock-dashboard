import yfinance as yf
import pandas as pd

df = yf.download("AAPL", period="1d", interval="5m")
df.reset_index(inplace=True)

df.to_csv("data/test.csv", index=False)

print("Data updated!")