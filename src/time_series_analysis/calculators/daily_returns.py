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
        """Fetch stock data and return with output schema columns."""
        df = self._fetch_stock_data()
        return df
