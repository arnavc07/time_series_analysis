from abc import ABC, abstractmethod

import polars as pl

from time_series_analysis.calculator_config.base import BaseConfig


class CalculatorBase(ABC):
    def __init__(self, config: BaseConfig):
        self.config = config

    @abstractmethod
    def output_schema(self) -> dict[str, pl.DataType]:
        ...

    @abstractmethod
    def calculate(self) -> pl.DataFrame:
        ...

    def execute(self) -> pl.DataFrame:
        df = self.calculate()
        assert isinstance(df, pl.DataFrame)

        schema = self.output_schema()
        for col, expected_dtype in schema.items():
            if col not in df.columns:
                raise ValueError(f"Missing expected column: {col}")
            if df.schema[col] != expected_dtype:
                raise ValueError(
                    f"Column '{col}' has dtype {df.schema[col]}, expected {expected_dtype}"
                )

        return df.select(list(schema.keys()))
