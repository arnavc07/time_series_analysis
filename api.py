import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

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

    def grouped_log_returns(self, col_names: list[str], groupby_cols: list[str]):
        "given a list of columns and a column to group by, compute the log returns and add them to the DataFrame."

        def _column_log_returns(df):
            df = df.set_index("Date").sort_index()
            for col_name in col_names:
                df[f"{col_name}_log_return"] = np.log(df[col_name] / df[col_name].shift(1)).dropna()

        self.df = (
            self.df.groupby(groupby_cols)[col_names + ["Date"]]
            .apply(_column_log_returns)
            .reset_index()
        )

    def cumulative_log_returns(self, col_names: list[str] = None, groupers: list[str] = None):
        "given a list of columns, compute the cumulative log returns and add them to the DataFrame. If col_names is none, computes cumulative returns for all log return columns"

        log_return_cols = [
            col_name for col_name in self.df.columns if "log_return" in col_name
        ]

        if col_names is not None:
            log_return_cols = col_names

        assert (
            len(log_return_cols) > 0
        ), "No log return columns found in the DataFrame, add log returns first"

        def _cumulative_returns(df):
            df = df.set_index("Date").sort_index()
            for col_name in log_return_cols:
                df[f"{col_name}_cumulative"] = df[col_name].cumsum()
            
            return df

        self.df = self.df.groupby(groupers)[col_names + ["Date"]].apply(_cumulative_returns)

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

    def returns_correlation_matrix(self) -> pd.DataFrame:
        "compute the correlation matrix of the log returns and return it as a DataFrame"
        cols = [
            col
            for col in self.df.columns
            if "log_return" in col and "cumulative" not in col
        ]
        return self.df[cols].corr()

    def plot_return_correlation_matrix(self):
        "plot the correlation matrix of the log returns."
        corr_df = self.returns_correlation_matrix()
        plt.figure(figsize=(12, 8))
        sns.heatmap(corr_df, annot=True, cmap="coolwarm", vmin=0.0, vmax=1, center=0.5)
        plt.title("Return Correlation Matrix")
        plt.show()

    def pairs_scatter_plot(self, x_col: str, y_col: str, **kwargs):
        self.df.plot.scatter(x=x_col, y=y_col, **kwargs)

    def realized_close_close_volatility_calculator(self) -> pd.DataFrame:
        "function to calculate the realized close-close volatility of a given dataframe of log returns"
        mean_log_return = self.df.mean()
        n_samples = self.df.shape[0]
        variance_of_log_returns = ((self.df - mean_log_return) ** 2).sum() / (
            n_samples - 1
        )
        annualized_variance = variance_of_log_returns * 252
        annualized_volatility = np.sqrt(annualized_variance)

        return annualized_volatility

    def rolling_realized_volatility_calculator(
        self, returns_col_name: str, window: int, groupers: list[str] = None
    ) -> pd.DataFrame:
        "function to calculate the rolling realized close-close volatility of a given dataframe of log returns, optionally grouped by a list of tickers"

        if groupers is not None:
            rolling_window = self.df.groupby(groupers)[[returns_col_name]].rolling(
                window=window
            )
        else:
            rolling_window = self.df[[returns_col_name]].rolling(window=window)

        return (
            rolling_window.apply(
                lambda x: self.realized_close_close_volatility_calculator(x),
                raw=True,
            )
            .dropna()
            .rename(
                columns={
                    returns_col_name: f"{returns_col_name}_realized_volatility_{window}"
                }
            )
        )

    def __call__(self):
        return self.df
