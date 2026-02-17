from pydantic import field_validator

from time_series_analysis.calculator_config.base import BaseConfig


class YFinanceStockDataCalculatorConfig(BaseConfig):
    tickers: list[str]

    @field_validator("tickers")
    @classmethod
    def tickers_must_not_be_empty(cls, v: list[str]) -> list[str]:
        if len(v) == 0:
            raise ValueError("tickers must contain at least one entry")
        return v
