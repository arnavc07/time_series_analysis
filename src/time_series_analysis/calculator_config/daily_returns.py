from pydantic import field_validator

from time_series_analysis.calculator_config.base import BaseConfig


class DailyReturnsCalculatorConfig(BaseConfig):
    """Configuration for the DailyReturnsCalculator.

    Attributes:
        start: First date to include in output (inherited).
        end: Last date to include in output (inherited).
        tickers: List of stock ticker symbols to compute returns for.
    """

    tickers: list[str]

    @field_validator("tickers")
    @classmethod
    def tickers_must_not_be_empty(cls, v: list[str]) -> list[str]:
        """Validate that at least one ticker is provided."""
        if not v:
            raise ValueError("tickers must contain at least one entry")
        return v
