from datetime import timedelta

import polars as pl

from time_series_analysis.calculator_config.daily_returns import (
    DailyReturnsCalculatorConfig,
)
from time_series_analysis.calculator_config.yfinance_stock_data import (
    YFinanceStockDataCalculatorConfig,
)
from time_series_analysis.calculators.base import CalculatorBase
from time_series_analysis.calculators.yfinance_stock_data import (
    YFinanceStockDataCalculator,
)


def compute_returns(df: pl.DataFrame) -> pl.DataFrame:
    """Compute log and arithmetic returns from a DataFrame with CLOSE prices.

    Args:
        df: DataFrame with columns BUSINESS_DATE, TICKER, CLOSE.

    Returns:
        DataFrame with additional columns LOG_RETURN and ARITHMETIC_RETURN.
        The first row per ticker will have null returns (no prior close).
    """
    return (
        df.with_columns(
            PREV_CLOSE=pl.col("CLOSE").shift(1).over("TICKER"),
        )
        .with_columns(
            LOG_RETURN=pl.col("CLOSE").log() - pl.col("PREV_CLOSE").log(),
            ARITHMETIC_RETURN=(pl.col("CLOSE") - pl.col("PREV_CLOSE"))
            / pl.col("PREV_CLOSE"),
        )
        .drop("PREV_CLOSE")
    )


class DailyReturnsCalculator(CalculatorBase):
    """Calculator that computes daily log and arithmetic returns for stock tickers.

    Fetches close prices via YFinanceStockDataCalculator, computes returns
    using the prior day's close, and outputs a validated Polars DataFrame
    filtered to the configured [start, end] date range.
    """

    def __init__(self, config: DailyReturnsCalculatorConfig) -> None:
        super().__init__(config)

    def output_schema(self) -> dict[str, pl.DataType]:
        """Return the expected output schema."""
        return {
            "BUSINESS_DATE": pl.Date,
            "TICKER": pl.Utf8,
            "CLOSE": pl.Float64,
            "LOG_RETURN": pl.Float64,
            "ARITHMETIC_RETURN": pl.Float64,
        }

    def _fetch_stock_data(self) -> pl.DataFrame:
        """Fetch close prices with a 7-day buffer for t-1 return computation."""
        stock_config = YFinanceStockDataCalculatorConfig(
            start=self.config.start - timedelta(days=7),
            end=self.config.end,
            tickers=self.config.tickers,
        )
        df = YFinanceStockDataCalculator(stock_config).execute()
        return df.sort("TICKER", "BUSINESS_DATE")

    def calculate(self) -> pl.DataFrame:
        """Fetch stock data, compute returns, and filter to configured date range."""
        df = self._fetch_stock_data()
        df = compute_returns(df)
        df = df.filter(
            (pl.col("BUSINESS_DATE") >= self.config.start.date())
            & (pl.col("BUSINESS_DATE") <= self.config.end.date())
        )
        return df
