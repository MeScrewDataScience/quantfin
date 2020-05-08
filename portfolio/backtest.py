# -*- coding: utf-8 -*-

# Import third-party libraries
import pandas as pd

# Import local module
import quantfin.portfolio.evaluation as eval

class MeanRevert():
    def __init__(self, dataframe):
        self.data = dataframe
        self.result = pd.DataFrame()

class Momentum():
    def __init__(self, dataframe):
        self.data = dataframe
        self.result = pd.DataFrame()

class Backtest():
    def __init__(self, dataframe):
        self.data = dataframe