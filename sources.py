import pandas as pd
import yfinance as yf


def get_single_ticker_price_history(
    ticker: str, start_timestamp: pd.Timestamp, end_timestamp: pd.Timestamp
) -> pd.DataFrame:
    """
    leverages the yfinance api to get the price history for a single ticker.
    the output schema is a dataframe with the following columns:

    - Date: the date of the price
    - Ticker: the ticker of the stock
    - Close: the closing price of the stock or index - this is adjusted for splits and dividends
    - Dividends: any dividends paid out for the stock for the day
    - High: the high price of the underlying/index for the given day
    - Low: the low price of the underlying/index for the given day
    - Open: the opening price of the underlying/index for the give day
    - Stock Splits: any stock splits that occurred on the given day
    - Volume: the total volume traded of the stock on the given day
    """

    price_ticker = yf.Ticker(ticker)
    price_history = price_ticker.history(start=start_timestamp, end=end_timestamp)

    return (
        price_history.reset_index()
        .melt(id_vars="Date")
        .assign(Ticker=lambda df: ticker)
        .rename(columns={"variable": "Price"})
        .pivot(index=["Date", "Ticker"], columns="Price", values="value")
        .reset_index()
    )


def get_price_history(
    tickers: list[str], start_timestamp: pd.Timestamp, end_timestamp: pd.Timestamp
) -> pd.DataFrame:
    """
    leverages the yfinance api to get the price history for a list of tickers.
    the output schema is a dataframe with the following columns:

    - Date: the date of the price
    - Ticker: the ticker of the stock
    - Close: the closing price of the stock or index - this is adjusted for splits and dividends
    - Dividends: any dividends paid out for the stock for the day
    - High: the high price of the underlying/index for the given day
    - Low: the low price of the underlying/index for the given day
    - Open: the opening price of the underlying/index for the give day
    - Stock Splits: any stock splits that occurred on the given day
    - Volume: the total volume traded of the stock on the given day
    """

    assert (
        len(tickers) > 1
    ), "Please provide more than one ticker to fetch data for, else use get_single_ticker_price_history"

    tickers_str = " ".join(tickers) if len(tickers) > 1 else tickers[0]
    print(tickers_str)

    price_tickers = yf.Tickers(tickers_str)
    price_history = price_tickers.history(start=start_timestamp, end=end_timestamp)

    return (
        price_history.reset_index()
        .melt(id_vars="Date")
        .pivot(index=["Date", "Ticker"], columns="Price", values="value")
        .reset_index()
    )
