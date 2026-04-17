import os
from datetime import datetime, timezone

import pandas as pd
import yfinance as yf

TICKERS = ["AAPL", "MSFT", "GOOGL", "SPY"]
OUTPUT_PATH = "data/final_stock_data.csv"


def flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [
            col[0] if isinstance(col, tuple) and len(col) > 0 else col
            for col in df.columns
        ]
    return df


def fetch_ticker(ticker: str) -> pd.DataFrame:
    df = yf.download(
        ticker,
        period="1d",
        interval="5m",
        progress=False,
        auto_adjust=False,
        group_by="column",
    )

    if df.empty:
        print(f"No data for {ticker}")
        return pd.DataFrame()

    df = flatten_columns(df)
    df = df.reset_index()

    if "Datetime" not in df.columns and "Date" in df.columns:
        df = df.rename(columns={"Date": "Datetime"})

    if "Datetime" not in df.columns:
        raise ValueError(f"No Datetime/Date column found for {ticker}. Columns: {list(df.columns)}")

    price_col = "Adj Close" if "Adj Close" in df.columns else "Close"

    required_cols = ["Open", "High", "Low", "Close", "Volume", price_col]
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        raise ValueError(f"Missing columns for {ticker}: {missing}. Columns: {list(df.columns)}")

    df["Ticker"] = ticker
    df["Return"] = df[price_col].pct_change()
    df["MA_5"] = df[price_col].rolling(5).mean()
    df["MA_20"] = df[price_col].rolling(20).mean()
    df["Price_Change"] = df[price_col].diff()
    df["High_Low_Spread"] = df["High"] - df["Low"]
    df["Ingestion_Timestamp_UTC"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    df["Price_Used"] = price_col

    output_cols = [
        "Datetime",
        "Ticker",
        "Open",
        "High",
        "Low",
        "Close",
        "Volume",
        "Return",
        "MA_5",
        "MA_20",
        "Price_Change",
        "High_Low_Spread",
        "Price_Used",
        "Ingestion_Timestamp_UTC",
    ]

    if "Adj Close" in df.columns:
        output_cols.insert(6, "Adj Close")

    return df[output_cols].copy()


def main() -> None:
    os.makedirs("data", exist_ok=True)

    all_data = []

    for ticker in TICKERS:
        print(f"Fetching {ticker}...")
        try:
            df = fetch_ticker(ticker)
            if not df.empty:
                all_data.append(df)
        except Exception as e:
            print(f"Failed for {ticker}: {e}")

    if not all_data:
        print("No data fetched for any ticker.")
        return

    final_df = pd.concat(all_data, ignore_index=True)
    final_df = final_df.sort_values(["Ticker", "Datetime"]).reset_index(drop=True)
    final_df.to_csv(OUTPUT_PATH, index=False)

    print(f"Clean data saved to {OUTPUT_PATH}")
    print(final_df.head())
    print(final_df.columns.tolist())


if __name__ == "__main__":
    main()