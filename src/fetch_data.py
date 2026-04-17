import os
from datetime import datetime, timezone

import pandas as pd
import yfinance as yf


TICKERS = ["AAPL", "MSFT", "GOOGL", "SPY"]
PERIOD = "1d"
INTERVAL = "5m"
OUTPUT_PATH = "data/final_stock_data.csv"


def download_ticker_data(ticker: str) -> pd.DataFrame:
    df = yf.download(
        ticker,
        period=PERIOD,
        interval=INTERVAL,
        progress=False,
        auto_adjust=False,
        group_by="column",
    )

    if df.empty:
        print(f"No data returned for {ticker}")
        return pd.DataFrame()

    df = df.reset_index()

    if "Datetime" not in df.columns and "Date" in df.columns:
        df = df.rename(columns={"Date": "Datetime"})

    df["Ticker"] = ticker

    close_col = "Adj Close" if "Adj Close" in df.columns else "Close"

    df["Return"] = df[close_col].pct_change()
    df["MA_5"] = df[close_col].rolling(5).mean()
    df["MA_20"] = df[close_col].rolling(20).mean()
    df["Price_Change"] = df[close_col].diff()
    df["High_Low_Spread"] = df["High"] - df["Low"]

    df["Ingestion_Timestamp_UTC"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    ordered_cols = [
        "Datetime",
        "Ticker",
        "Open",
        "High",
        "Low",
        "Close",
        "Adj Close" if "Adj Close" in df.columns else "Close",
        "Volume",
        "Return",
        "MA_5",
        "MA_20",
        "Price_Change",
        "High_Low_Spread",
        "Ingestion_Timestamp_UTC",
    ]

    final_cols = []
    seen = set()
    for col in ordered_cols:
        if col in df.columns and col not in seen:
            final_cols.append(col)
            seen.add(col)

    return df[final_cols].copy()


def main() -> None:
    os.makedirs("data", exist_ok=True)

    all_frames = []

    for ticker in TICKERS:
        print(f"Downloading {ticker}...")
        ticker_df = download_ticker_data(ticker)
        if not ticker_df.empty:
            all_frames.append(ticker_df)

    if not all_frames:
        print("No data returned for any ticker. Existing file not overwritten.")
        return

    final_df = pd.concat(all_frames, ignore_index=True)

    final_df = final_df.sort_values(["Ticker", "Datetime"]).reset_index(drop=True)
    final_df.to_csv(OUTPUT_PATH, index=False)

    print(f"Saved {len(final_df)} rows to {OUTPUT_PATH}")
    print(final_df.tail(10))


if __name__ == "__main__":
    main()