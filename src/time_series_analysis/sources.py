import csv
import pandas as pd
import yfinance as yf
import requests
import time


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

    df = (
        price_history.melt(ignore_index=False)
        .reset_index()
        .pivot(index=["Ticker", "Date"], columns="Price")
        .reset_index()
    )

    df.columns = [col[1] if col[1] != "" else col[0] for col in df.columns]
    return df


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


class PolygonIo:
    BASE_URL = "https://api.polygon.io/v2"
    STOCKS_AGG_DATA_URL = "aggs/grouped/locale/us/market/stocks"

    def __init__(self, api_key: str = "XkidfP9emVjqagKXvkWPUt_mJae6qvn9"):
        self._api_key = api_key

    @property
    def api_key(self):
        return self._api_key

    def create_grouped_bars_url(self, date: str, adjusted: bool = True) -> str:
        """
        Creates a URL for fetching grouped bars data from the Polygon API.

        Parameters:
        - date (str): The date for which the data is to be fetched.
        - adjusted (bool): Whether the data should be adjusted for splits (default: True).

        Returns:
        - str: The URL for fetching the grouped bars data.
        """
        adjusted_str = "true" if adjusted else "false"
        formatted_date = date.strftime("%Y-%m-%d")
        return f"{self.BASE_URL}/{self.STOCKS_AGG_DATA_URL}/{formatted_date}?adjusted={adjusted_str}&apiKey={self.api_key}"

    def get_end_of_day_stock_summary(
        self, start_date, end_date, adjust_for_splits=True, return_raw_response=False
    ):
        """
        Fetches the volume for all tickers in the stock market for a given date range.

        Parameters:
        - start_date (str): The start date of the date range in the format 'YYYY-MM-DD'.
        - end_date (str): The end date of the date range in the format 'YYYY-MM-DD'.

        Returns:
        - pandas.DataFrame: A DataFrame containing the volume data for all tickers.

        Note:
        - This method makes synchronous API calls to fetch the volume data for each date in the given range.
        - If an API call fails for a specific date, it will be skipped and the process will continue.
        """
        date_range = pd.date_range(start=start_date, end=end_date, freq="B")
        volume_data = []

        counter = 0
        # TODO: make this call asynchronous
        for date in date_range:
            print(f"processing {date}")
            formatted_date = date.strftime("%Y-%m-%d")
            url = self.create_grouped_bars_url(date, adjust_for_splits)
            response = requests.get(url)
            if return_raw_response:
                return response

            if response.status_code == 200:
                data = response.json()
                if "results" in data:
                    for results in data["results"]:
                        data = {
                            "Date": formatted_date,
                            "Ticker": results["T"],
                            "Volume": results["v"],
                            "Open": results["o"],
                            "Close": results["c"],
                            "High": results["h"],
                            "Low": results["l"],
                            "Timestamp": results["t"],
                            "Timestamp_cst": pd.Timestamp(results["t"] * 1000000)
                            .tz_localize("UTC")
                            .tz_convert("US/Central"),  # convert millis to nanos
                            "VWAP": 0.0,  # default, updated later if the field exists
                            "Num_Trades": 0.0,  # default, updated later if the field exists
                        }

                        # these fields can be excluded if traded volume in the stock is 0 for the day
                        if "vw" in results:
                            data.update({"VWAP": results["vw"]})
                        if "n" in results:
                            data.update({"Num_Trades": results["n"]})
                        volume_data.append(data)

            else:
                print(
                    f"Failed to fetch data for {formatted_date}. Status Code: {response.status_code}"
                )
                continue

            counter += 1
            if counter % 5 == 0:
                print("throttling API requests for 60 seconds")
                time.sleep(70)
                print("resuming API requests")

        df = pd.DataFrame(volume_data)
        df.columns = [col_name.upper() for col_name in df.columns]

        return df
