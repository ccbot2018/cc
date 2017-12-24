import os.path
import pandas as pd
import datetime as dt
import numpy as np

from Core.ExchangeData import Frequency
from Core.CurrencyPairs import CurrencyPair

class DataProvider:
    def __init__(self, exchange, cacheFolder, frequency, pairsDict, refCurrency):
        self.Exchange = exchange
        self.ExchangeName = exchange.Name
        self.CacheFolder = cacheFolder
        self.Frequency = frequency
        self.Markets = pairsDict
        self.CloseDataStorage = pd.DataFrame()
        self.DisableCalls = False
        self.RefCurrency = refCurrency
        self.ToRefCcy = self.__toRefCurrencyDict()

    def RefreshCache(self, frequency):
        for pair, pairValue in self.Markets.items():
            self.RefreshPairTimeSerie(pair, frequency)

    def LoadCachedClose(self):
        for pair, pairValue in self.Markets.items():
            data =self.__getCachedPairData(pair, self.Frequency)
            self.CloseDataStorage[str(pair)] = data['Close']
        self.CloseDataStorage =  self.CloseDataStorage.fillna(method='ffill')
        self.CloseDataStorage = self.CloseDataStorage.fillna(method='bfill')
        self.__rebaseToRefCcy()

    def GetTimeSerieClose(self, currency):
        if not self.DisableCalls:
            self.GetSnapshotDataAllMarkets()
        return self.CloseDataStorage[currency]

    def GetCurrentClose(self, currency):
        if not self.DisableCalls:
            self.GetSnapshotDataAllMarkets()
        return self.CloseDataStorage[currency].tail(1)[0]

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

    def GetSnapshotDataAllMarkets(self):
        allMarkets = self.Exchange.GetMarketsSnapshot()
        stampDate = dt.datetime.now()
        emptyRow = np.empty((1, len(self.CloseDataStorage.columns)))
        emptyRow[:] = np.nan
        emptyRow_df = pd.DataFrame(emptyRow, columns= self.CloseDataStorage.columns)
        emptyRow_df = emptyRow_df.set_index(pd.DatetimeIndex([stampDate]))
        self.CloseDataStorage = self.CloseDataStorage.append(emptyRow_df)
        for pair in self.Markets:
            self.__cacheSnapshotPair(pair, allMarkets[pair], stampDate)
        self.__rebaseToRefCcy()

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
        self.CloseDataStorage[str(currencyPair)][-1] = exchangePairMarketData.Last
        self.CloseDataStorage[str(currencyPair)] = self.CloseDataStorage[str(currencyPair)].fillna(method='ffill')
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

    def __rebaseToRefCcy(self):
        for ccy, ccyPath in self.ToRefCcy.items():
            self.CloseDataStorage[ccy] = 1.0
            for pair in ccyPath:
                if pair[0] == "Long":
                    self.CloseDataStorage[ccy] *= self.CloseDataStorage[str(pair[1])]
                elif pair[0] == "Short":
                    self.CloseDataStorage[ccy] *= 1.0 / self.CloseDataStorage[str(pair[1])]

    def __extractCurrencyList(self):
        currencies = set()
        baseCurrencies = set()
        for pairName, pair in self.Markets.items():
            if not pairName.BaseCurrency in currencies:
                currencies.add(pairName.BaseCurrency)
            if not pairName.BaseCurrency in baseCurrencies:
                baseCurrencies.add(pairName.BaseCurrency)
            if not pairName.MarketCurrency in currencies:
                currencies.add(pairName.MarketCurrency)
        return currencies, baseCurrencies

    def __toRefCurrencyDict(self):
        retVal = dict()
        currencies, baseCurrencies = self.__extractCurrencyList()
        for baseCurrency in baseCurrencies:
            ccyPair = CurrencyPair(self.RefCurrency, baseCurrency)
            if (ccyPair in self.Markets):
                retVal[baseCurrency] = list([("Long", ccyPair)])
            elif (ccyPair.GetReversePair() in self.Markets):
                retVal[baseCurrency] = list([("Short", ccyPair.GetReversePair())])
            elif ccyPair.BaseCurrency == self.RefCurrency:
                retVal[baseCurrency] = list([("Identity")])
        for currency in currencies:
            for baseCurrency in baseCurrencies:
                basePair = CurrencyPair(baseCurrency, currency)
                if basePair in self.Markets:
                    pivotToRef = retVal[baseCurrency][0]
                    retVal[currency] = list([("Long", basePair)])
                    retVal[currency].append(pivotToRef)
        return retVal


