import datetime as dt
import pandas as pd
from Core.ExchangeData import Frequency
from Core.BacktestingExchangeBook import BacktestingExchangeBook

FREQUENCIES_DICT = {Frequency.min : dt.timedelta(0,60),
                        Frequency.fivemin : dt.timedelta(0,360),
                        Frequency.thirtymin : dt.timedelta(0, 1800),
                        Frequency.hour : dt.timedelta(0,3600),
                        Frequency.day : dt.timedelta(1)}

class BackTestingMonoExchange:
    def __init__(self, startDate, endDate, frequency, strategy, dataProvider, fees, bidAskSpread):
        self.StartDate = startDate
        self.EndDate= endDate
        self.Frequency = frequency
        self.Strategy = strategy
        self.DataProvider = dataProvider
        self.DataProvider.DisableCalls = True
        self.DataProvider.LoadCachedClose()
        self.DataUniverse = self.DataProvider.CloseDataStorage.copy()
        self.Fees = fees
        self.BidAskSpread = bidAskSpread

    def SetUp(self):
        book = BacktestingExchangeBook({"USDT":1000}, self.Fees, self.BidAskSpread)
        self.Strategy.Book = book
        self.Strategy.DataProvider = self.DataProvider

    def Start(self):
        tempDate = self.StartDate
        mtm = dict()
        while tempDate < self.EndDate:
            print (tempDate)
            updatedUniverse = self.DataUniverse.loc[
                self.DataUniverse.index < tempDate]
            self.Strategy.DataProvider.CloseDataStorage = updatedUniverse
            mtm[tempDate] = self.Strategy.Update()
            tempDate = tempDate + FREQUENCIES_DICT[self.Frequency]
        return pd.DataFrame.from_dict(mtm, orient= 'index')

