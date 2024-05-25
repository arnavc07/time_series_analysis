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

    