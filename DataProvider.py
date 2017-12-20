import os.path
import pandas as pd

from Base.CurrencyPairs import CurrencyPair
from TimeSerie import TimeSerie



class DataProvider:
    def __init__(self, exchange, cacheFolder):
        self.Exchange = exchange
        self.ExchangeName = exchange.Name
        self.CacheFolder = cacheFolder

    def RefreshCache(self, pairsDictionary, frequency):
        for pair, pairValue in pairsDictionary.items():
            self.RefreshPairTimeSerie(pair, frequency)

    def __pairCacheFileName(self, ticker, frequency):
        return os.path.join(self.CacheFolder,self.ExchangeName, str(ticker) +"_"+frequency.value+".csv" )

    def GetPairTimeSerie(self, currencyPair, frequency, startDate, endDate):
        print("Retrieving " + str(currencyPair))
        fileName = self.__pairCacheFileName(currencyPair, frequency)
        cacheData_df = pd.DataFrame()
        if os.path.isfile(fileName):
            cacheData_df = pd.read_csv(fileName, index_col= 0,
                                header = None, names= ["BaseVolume","Close", "High", "Low", "Open","Volume"],
                                parse_dates= True)
        filteredData_df = cacheData_df.loc[startDate<=cacheData_df.index]
        filteredData_df = filteredData_df.loc[filteredData_df.index <= endDate]
        return TimeSerie(currencyPair,self.ExchangeName, frequency, filteredData_df)

    def RefreshPairTimeSerie(self, currencyPair, frequency):
        print("Refreshing " + str(currencyPair))
        fileName = self.__pairCacheFileName(currencyPair, frequency)
        exchangeData_df = self.Exchange.GetCurrencyPairTimeSerie(currencyPair, frequency)
        cacheData_df = pd.DataFrame()
        if os.path.isfile(fileName):
            cacheData_df = pd.read_csv(fileName, index_col= 0,
                                header = None, names= ["BaseVolume","Close", "High", "Low", "Open","Volume"],
                                parse_dates= True)
        if not cacheData_df.empty:
            lastCacheDate = cacheData_df.index.values[-1]
            exchangeData_df = exchangeData_df.loc[exchangeData_df.index > lastCacheDate]
            with open(fileName, 'a') as f:
                exchangeData_df.to_csv(f, header=False)
            cacheData_df = cacheData_df.append(exchangeData_df)
        else:
            cacheData_df = exchangeData_df
            with open(fileName, 'w') as f:
                exchangeData_df.to_csv(f, header=False)
        return TimeSerie(currencyPair,self.ExchangeName, frequency, cacheData_df)



