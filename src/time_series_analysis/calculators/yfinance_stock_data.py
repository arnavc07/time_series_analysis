import pandas as pd
import polars as pl

from time_series_analysis.calculator_config.yfinance_stock_data import (
    YFinanceStockDataCalculatorConfig,
)
from time_series_analysis.calculators.base import CalculatorBase
from time_series_analysis.sources import (
    get_price_history,
    get_single_ticker_price_history,
)

COLUMN_RENAME_MAP = {
    "Date": "BUSINESS_DATE",
    "Ticker": "TICKER",
    "Close": "CLOSE",
    "Dividends": "DIVIDENDS",
    "High": "HIGH",
    "Low": "LOW",
    "Open": "OPEN",
    "Stock Splits": "STOCK_SPLITS",
    "Volume": "VOLUME",
}


class YFinanceStockDataCalculator(CalculatorBase):
    def __init__(self, config: YFinanceStockDataCalculatorConfig):
        super().__init__(config)

    def output_schema(self) -> dict[str, pl.DataType]:
        return {
            "BUSINESS_DATE": pl.Date,
            "TICKER": pl.Utf8,
            "CLOSE": pl.Float64,
            "DIVIDENDS": pl.Float64,
            "HIGH": pl.Float64,
            "LOW": pl.Float64,
            "OPEN": pl.Float64,
            "STOCK_SPLITS": pl.Float64,
            "VOLUME": pl.Float64,
        }

    def calculate(self) -> pl.DataFrame:
        start = pd.Timestamp(self.config.start)
        end = pd.Timestamp(self.config.end)

        if len(self.config.tickers) == 1:
            pdf = get_single_ticker_price_history(self.config.tickers[0], start, end)
        else:
            pdf = get_price_history(self.config.tickers, start, end)

        df = pl.from_pandas(pdf)
        df = df.rename(COLUMN_RENAME_MAP)
        df = df.with_columns(
            pl.col("BUSINESS_DATE").cast(pl.Date),
            pl.col("CLOSE").cast(pl.Float64),
            pl.col("DIVIDENDS").cast(pl.Float64),
            pl.col("HIGH").cast(pl.Float64),
            pl.col("LOW").cast(pl.Float64),
            pl.col("OPEN").cast(pl.Float64),
            pl.col("STOCK_SPLITS").cast(pl.Float64),
            pl.col("VOLUME").cast(pl.Float64),
        )

        return df
