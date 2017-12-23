import os.path
import pandas as pd
import datetime as dt
import numpy as np

from Core.ExchangeData import Frequency

class DataProvider:
    def __init__(self, exchange, cacheFolder, frequencyList, pairsDict):
        self.Exchange = exchange
        self.ExchangeName = exchange.Name
        self.CacheFolder = cacheFolder
        self.FrequencyList = frequencyList
        self.Markets = pairsDict
        self.CloseDataStorage = dict()

    def RefreshCache(self, frequency):
        for pair, pairValue in self.Markets.items():
            self.RefreshPairTimeSerie(pair, frequency)

    def LoadCachedClose(self):
        for frequency in self.FrequencyList:
            self.CloseDataStorage[frequency] = pd.DataFrame()
            for pair, pairValue in self.Markets.items():
                data =self.__getCachedPairData(pair, frequency)
                self.CloseDataStorage[frequency][pair] = data['Close']
            self.CloseDataStorage[frequency] =  self.CloseDataStorage[frequency].fillna(method='bfill')

    def GetTimeSerieClose(self, currencyPair, frequency):
        self.GetSnapshotDataAllMarkets(True)
        return self.CloseDataStorage[frequency][currencyPair]

    def RefreshPairTimeSerie(self, currencyPair, frequency):
        print("Refreshing " + str(currencyPair) + " " + str(frequency))
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
        else:
            with open(fileName, 'w') as f:
                exchangeData_df.to_csv(f, header=False)

    def GetSnapshotDataAllMarkets(self, shouldCache):
        allMarkets = self.Exchange.GetMarketsSnapshot()
        stampDate = dt.datetime.now()
        if shouldCache:
            for freq in self.FrequencyList:
                emptyRow = np.empty((1, len(self.CloseDataStorage[freq].columns)))
                emptyRow[:] = np.nan
                emptyRow_df = pd.DataFrame(emptyRow, columns= self.CloseDataStorage[freq].columns)
                emptyRow_df = emptyRow_df.set_index(pd.DatetimeIndex([stampDate]))
                self.CloseDataStorage[freq] = self.CloseDataStorage[freq].append(emptyRow_df)
            for pair in self.Markets:
                self.__cacheSnapshotPair(pair, allMarkets[pair], stampDate)
        return allMarkets

    def __getCachedPairData(self, currencyPair, frequency):
        print("Retrieving " + str(currencyPair) + " " + str(frequency))
        fileName = self.__pairCacheFileName(currencyPair, frequency)
        cacheData_df = pd.DataFrame()
        if frequency is not Frequency.snapshot:
            if os.path.isfile(fileName):
                cacheData_df = pd.read_csv(fileName, index_col=0,
                                           header=None, names=["BaseVolume", "Close", "High", "Low", "Open", "Volume"],
                                           parse_dates=True)
        else:
            cacheData_df = pd.read_csv(fileName, index_col=0,
                                               header=None, names=["MinTradeSize", "Ask", "Bid", "Close", "Mid"],
                                               parse_dates=True)
        return cacheData_df

    def __cacheSnapshotPair(self, currencyPair, exchangePairMarketData, stampDate):
        fileName = self.__pairCacheFileName(currencyPair, Frequency.snapshot)
        for freq in self.FrequencyList:
            self.CloseDataStorage[freq][currencyPair][-1] = exchangePairMarketData.Last
            self.CloseDataStorage[freq][currencyPair] = self.CloseDataStorage[freq][currencyPair].fillna(method='bfill')
        cacheData_df = pd.DataFrame()
        if os.path.isfile(fileName):
            cacheData_df = pd.read_csv(fileName, index_col=0,
                                       header=None, names=["MinTradeSize", "Ask", "Bid", "Last", "Mid"],
                                       parse_dates=True)
        if not cacheData_df.empty:
            with open(fileName, 'a') as f:
                f.writelines(
                    [stampDate.strftime("%Y-%m-%d %H:%M:%S") + "," + exchangePairMarketData.ToString() + "\n"])
        else:
            with open(fileName, 'w') as f:
                f.writelines(
                    [stampDate.strftime("%Y-%m-%d %H:%M:%S") + "," + exchangePairMarketData.ToString() + "\n"])

    def __pairCacheFileName(self, ticker, frequency):
        return os.path.join(self.CacheFolder,self.ExchangeName, str(ticker) +"_"+frequency.value+".csv" )


