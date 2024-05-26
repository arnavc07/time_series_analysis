import pandas as pd
import numpy as np


class TimeSeriesApi:
    """
    A utility class for time series data manipulation, visualization and analysis.
    The class is designed to be used in a Jupyter notebook environment and operates on a pandas DataFrame.
    """

    def __init__(self, df: pd.DataFrame):
        self.df = df

    @property
    def df(self):
        return self._df

    @df.setter
    def df(self, df):
        self._df = df

    def log_returns(self, col_names: list[str]):
        "given a list of columns, compute the log returns and add them to the DataFrame."
        for col_name in col_names:
            self.df[f"{col_name}_log_return"] = np.log(
                self.df[col_name] / self.df[col_name].shift(1)
            )

    def cumulative_log_returns(self, col_names: list[str] = None):
        "given a list of columns, compute the cumulative log returns and add them to the DataFrame. If col_names is none, computes cumulative returns for all log return columns"

        log_return_cols = [
            col_name for col_name in self.df.columns if "log_return" in col_name
        ]
        assert (
            len(log_return_cols) > 0
        ), "No log return columns found in the DataFrame, add log returns first"

        for col_name in log_return_cols:
            self.df[f"{col_name}_cumulative"] = self.df[col_name].cumsum()

    def plot(self, col_names: list[str], **kwargs):
        self.df[col_names].plot(**kwargs)

    def line_plot(self, col_names: list[str], **kwargs):
        self.df[col_names].plot.line(**kwargs)

    def histogram(self, col_names: list[str], **kwargs):
        self.df[col_names].plot.hist(**kwargs)

    def simple_moving_average(self, col_names: list[str], windows: list[int]):
        "given a list of columns and a window size, compute the simple moving average and add it to the DataFrame."

        for window in windows:
            for col_name in col_names:
                self.df[f"{col_name}_sma_{window}"] = (
                    self.df[col_name].rolling(window=window).mean()
                )

    def exponential_moving_average(self, col_names: list[str], windows: list[int]):
        "given a list of columns and a window size, compute the exponential moving average and add it to the DataFrame."
        for window in windows:
            for col_name in col_names:
                self.df[f"{col_name}_ema_{window}"] = (
                    self.df[col_name].ewm(span=window, adjust=False).mean()
                )

    def __call__(self):
        return self.df
