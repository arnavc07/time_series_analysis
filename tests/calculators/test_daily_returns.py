import math
from datetime import date

import polars as pl
import pytest

from time_series_analysis.calculators.daily_returns import compute_returns


@pytest.fixture()
def two_ticker_df() -> pl.DataFrame:
    """Synthetic close-price data for two tickers across three dates."""
    return pl.DataFrame(
        {
            "BUSINESS_DATE": [
                date(2025, 1, 6),
                date(2025, 1, 7),
                date(2025, 1, 8),
                date(2025, 1, 6),
                date(2025, 1, 7),
                date(2025, 1, 8),
            ],
            "TICKER": ["AAPL", "AAPL", "AAPL", "MSFT", "MSFT", "MSFT"],
            "CLOSE": [100.0, 110.0, 121.0, 200.0, 190.0, 209.0],
        }
    )


class TestComputeReturns:
    def test_log_return_calculation(self, two_ticker_df: pl.DataFrame) -> None:
        """LOG_RETURN should equal ln(close / prev_close)."""
        result = compute_returns(two_ticker_df)
        aapl = result.filter(pl.col("TICKER") == "AAPL").sort("BUSINESS_DATE")
        log_returns = aapl["LOG_RETURN"].to_list()

        assert log_returns[1] == pytest.approx(math.log(110.0 / 100.0))
        assert log_returns[2] == pytest.approx(math.log(121.0 / 110.0))

    def test_arithmetic_return_calculation(self, two_ticker_df: pl.DataFrame) -> None:
        """ARITHMETIC_RETURN should equal (close - prev_close) / prev_close."""
        result = compute_returns(two_ticker_df)
        aapl = result.filter(pl.col("TICKER") == "AAPL").sort("BUSINESS_DATE")
        arith_returns = aapl["ARITHMETIC_RETURN"].to_list()

        assert arith_returns[1] == pytest.approx((110.0 - 100.0) / 100.0)
        assert arith_returns[2] == pytest.approx((121.0 - 110.0) / 110.0)

    def test_multiple_tickers_independent(self, two_ticker_df: pl.DataFrame) -> None:
        """Returns should be computed independently per ticker (no cross-ticker bleed)."""
        result = compute_returns(two_ticker_df)
        msft = result.filter(pl.col("TICKER") == "MSFT").sort("BUSINESS_DATE")
        log_returns = msft["LOG_RETURN"].to_list()

        # MSFT: 200 -> 190 -> 209
        assert log_returns[1] == pytest.approx(math.log(190.0 / 200.0))
        assert log_returns[2] == pytest.approx(math.log(209.0 / 190.0))

    def test_first_row_per_ticker_is_null(self, two_ticker_df: pl.DataFrame) -> None:
        """The first observation per ticker has no prior close, so returns are null."""
        result = compute_returns(two_ticker_df)

        for ticker in ["AAPL", "MSFT"]:
            first_row = (
                result.filter(pl.col("TICKER") == ticker).sort("BUSINESS_DATE").head(1)
            )
            assert first_row["LOG_RETURN"].is_null().all()
            assert first_row["ARITHMETIC_RETURN"].is_null().all()
