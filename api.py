import pandas as pd
import numpy as np


class TimeSeriesApi:
    def __init__(self, df: pd.DataFrame):
        self.df = df

    @property
    def df(self):
        return self._df

    @df.setter
    def df(self, df):
        self._df = df

    def log_returns(self, col_names: list[str]):
        for col_name in col_names:
            self.df[f"{col_name}_log_return"] = np.log(
                self.df[col_name] / self.df[col_name].shift(1)
            )

    def plot(self, col_names: list[str], **kwargs):
        self.df[col_names].plot(**kwargs)

    def line_plot(self, col_names: list[str], **kwargs):
        self.df[col_names].plot.line(**kwargs)

    def histogram(self, col_names: list[str], **kwargs):
        self.df[col_names].plot.hist(**kwargs)

    def __call__(self):
        return self.df
