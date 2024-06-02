import csv
import pandas as pd
import yfinance as yf
import requests


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


class AlphaVantageApi:
    def __init__(self, api_key: str = "UB63UD2IVOL1A8G1"):
        self.api_key = api_key

    def get_active_tickers(self) -> pd.DataFrame:
        """
        fetches the active tickers from the Alpha Vantage API
        """
        url = f"https://www.alphavantage.co/query?function=LISTING_STATUS&state=active&apikey={self.api_key}"

        with requests.Session() as s:
            download = s.get(url)
            decoded_content = download.content.decode("utf-8")
            cr = csv.reader(decoded_content.splitlines(), delimiter=",")
            my_list = list(cr)

            col_names = my_list[0]

            return pd.DataFrame.from_records(my_list[1:], columns=col_names)

    def get_most_actively_traded_tickers(self) -> pd.DataFrame:
        """
        fetches the most actively traded tickers from the Alpha Vantage API
        """
        url = f"https://www.alphavantage.co/query?function=TOP_GAINERS_LOSERS&state=active&apikey={self.api_key}"
        r = requests.get(url)
        return r.json()
