import polars as pl
from polars import Expr as F


class Metrics:
    @staticmethod
    def log_returns(
        df: pl.DataFrame, partition_col: str = "TICKER", close_col: str = "CLOSE"
    ) -> pl.DataFrame:
        """
        Calculate the logarithmic returns for a given DataFrame.

        Args:
            df (pl.DataFrame): The input DataFrame containing the 'Close' column.
            partition_col (str, optional): The column used for partitioning the DataFrame. Defaults to "Ticker".

        Returns:
            pl.DataFrame: The input DataFrame with additional columns 'prev_close' and 'log_return'.

        """
        return df.with_columns(
            PREV_CLOSE=pl.col(close_col).shift(1).over(partition_col)
        ).with_columns(LOG_RETURN=F.log(pl.col(close_col) / pl.col("PREV_CLOSE")))

    @staticmethod
    def abs_log_returns(
        df: pl.DataFrame, partition_col: str = "Ticker"
    ) -> pl.DataFrame:
        """
        Calculate the absolute log returns for each row in the DataFrame.

        Parameters:
        - df (pl.DataFrame): The input DataFrame containing the log returns.
        - partition_col (str): The column name to partition the DataFrame (default: "Ticker").

        Returns:
        - pl.DataFrame: The DataFrame with an additional column "abs_log_return" containing the absolute log returns.

        Example:
        >>> df = pl.DataFrame({
        ...     "Ticker": ["AAPL", "GOOGL", "MSFT"],
        ...     "log_return": [0.05, -0.02, 0.03]
        ... })
        >>> abs_log_returns(df)
           Ticker  log_return  abs_log_return
        0    AAPL        0.05            0.05
        1   GOOGL       -0.02            0.02
        2    MSFT        0.03            0.03
        """
        return df.with_columns(abs_log_return=pl.col("log_return").abs())

    @staticmethod
    def cumulative_returns(
        df: pl.DataFrame, partition_col: str = "Ticker"
    ) -> pl.DataFrame:
        """
        Calculates the cumulative returns for each partition in the DataFrame.

        Args:
            df (pl.DataFrame): The input DataFrame containing the data.
            partition_col (str, optional): The column used for partitioning the data. Defaults to "Ticker".

        Returns:
            pl.DataFrame: The DataFrame with an additional column "cumulative_return" containing the cumulative returns.
        """
        return df.with_columns(
            cumulative_return=pl.col("log_return").cum_sum().over(partition_col)
        )

    @staticmethod
    def mean_returns(df: pl.DataFrame, partition_col: str = "TICKER", log_return_col: str = "LOG_RETURN") -> pl.DataFrame:
        """
        Calculate the mean returns of a DataFrame.

        Args:
            df (pl.DataFrame): The input DataFrame containing the log returns.
            partition_col (str, optional): The column used for partitioning the data. Defaults to "Ticker".

        Returns:
            pl.DataFrame: The DataFrame with an additional column "mean_return" containing the mean returns.

        """
        return df.with_columns(
            MEAN_RETURN=pl.col(log_return_col).mean().over(partition_col)
        )

    @staticmethod
    def std_dev_returns(df: pl.DataFrame, partition_col: str = "TICKER", log_return_col: str = "LOG_RETURN") -> pl.DataFrame:
        """
        Calculate the standard deviation of the returns for each partition in the DataFrame.

        Args:
            df (pl.DataFrame): The input DataFrame containing the data.
            partition_col (str, optional): The column used for partitioning the data. Defaults to "Ticker".

        Returns:
            pl.DataFrame: The DataFrame with an additional column "std_dev_return" containing the standard deviation of the returns.
        """
        return df.with_columns(
            STD_DEV_RETURN=pl.col(log_return_col).std().over(partition_col)
        )

    @staticmethod
    def annualized_returns(
        df: pl.DataFrame,
        partition_col: str = "Ticker",
        annualization_factor: float = 252.0,
    ) -> pl.DataFrame:
        """
        Calculates the annualized returns for a given DataFrame.

        Parameters:
        - df (pl.DataFrame): The input DataFrame containing the data.
        - partition_col (str): The column used for partitioning the data (default: "Ticker").
        - annualization_factor (float): The factor used for annualization (default: 252.0).

        Returns:
        - pl.DataFrame: The DataFrame with the annualized returns calculated.
        """
        return df.with_columns(
            annualized_return=(
                (pl.lit(1.0) + pl.col("mean_return")) ** annualization_factor
                - pl.lit(1.0)
            ).over(partition_col)
        )

    @staticmethod
    def squared_diff(df: pl.DataFrame, partition_col: str = "Ticker") -> pl.DataFrame:
        """
        Calculates the squared difference between the log return and mean return for each partition.

        Args:
            df (pl.DataFrame): The input DataFrame containing the log return and mean return columns.
            partition_col (str, optional): The column used for partitioning the DataFrame. Defaults to "Ticker".

        Returns:
            pl.DataFrame: The input DataFrame with an additional column "sum_squared_diff" containing the squared differences.
        """
        return df.with_columns(
            sum_squared_diff=((pl.col("log_return") - pl.col("mean_return")) ** 2).over(
                partition_col
            )
        )

    @staticmethod
    def realized_variance(
        df: pl.DataFrame, partition_col: str = "Ticker"
    ) -> pl.DataFrame:
        """
        Calculates the realized variance for each partition in the DataFrame.

        Args:
            df (pl.DataFrame): The input DataFrame.
            partition_col (str, optional): The column used for partitioning the DataFrame. Defaults to "Ticker".

        Returns:
            pl.DataFrame: The DataFrame with an additional column "realized_variance" containing the calculated realized variance for each partition.
        """
        return df.with_columns(
            realized_variance=pl.col("sum_squared_diff").mean().over(partition_col)
        )

    @staticmethod
    def realized_volatility(
        df: pl.DataFrame, partition_col: str = "Ticker"
    ) -> pl.DataFrame:
        """
        Calculates the realized volatility of a time series DataFrame.

        Parameters:
        - df (pl.DataFrame): The input DataFrame containing the time series data.
        - partition_col (str): The column used for partitioning the data (default: "Ticker").

        Returns:
        - pl.DataFrame: The input DataFrame with an additional column "realized_volatility"
            containing the calculated realized volatility.

        """
        return df.with_columns(
            realized_volatility=pl.col("realized_variance").sqrt().over(partition_col)
        )

    @staticmethod
    def annualized_realized_volatility(
        df: pl.DataFrame,
        partition_col: str = "Ticker",
        annualization_factor: float = 252.0,
    ) -> pl.DataFrame:
        """
        Calculates the annualized realized volatility for a given DataFrame.

        Parameters:
        - df (pl.DataFrame): The input DataFrame containing the data.
        - partition_col (str): The column used for partitioning the data (default: "Ticker").
        - annualization_factor (float): The factor used for annualization (default: 252.0).

        Returns:
        - pl.DataFrame: The DataFrame with an additional column "annualized_realized_volatility" containing the calculated values.
        """
        return df.with_columns(
            annualized_realized_volatility=pl.col("realized_volatility")
            * (annualization_factor**0.5)
        )

    @staticmethod
    def sharpe_ratio(df: pl.DataFrame, partition_col: str = "Ticker"):
        """
        Calculates the Sharpe ratio for a given DataFrame.

        Args:
            df (pl.DataFrame): The DataFrame containing the necessary columns for the calculation.
            partition_col (str, optional): The column to partition the DataFrame by. Defaults to "Ticker".

        Returns:
            pl.DataFrame: The DataFrame with the Sharpe ratio column added.

        """
        return df.with_columns(
            sharpe_ratio=(
                pl.col("annualized_return")
                - pl.col("ANNUALIZED_RISK_FREE_RATE")
                / pl.col("annualized_realized_volatility")
            )
        )
