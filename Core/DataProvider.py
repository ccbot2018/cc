import os.path
import pandas as pd
import datetime as dt
import numpy as np
import json

from Core.Frequency import Frequency
from Core.CurrencyPairs import CurrencyPair
from Core.ExchangePairsHelper import CreateAllPossiblePairs, CreateAllPossibleTransfers

class DataProvider:
    def __init__(self, exchange, cacheFolder, frequency, pairsDict, preferredPivotCcy):
        self.Exchange = exchange
        self.ExchangeName = exchange.Name
        self.CacheFolder = cacheFolder
        self.Frequency = frequency
        self.Markets = pairsDict
        self.CloseDataStorage = pd.DataFrame()
        self.DisableCalls = False
        self.RefCurrency = exchange.RefCurrency
        self.ToRefCcy = self.__toRefCurrencyDict()
        currencies, baseCurrencies = self.__extractCurrencyList()
        self.CurrencyList = set(list(baseCurrencies) + list(currencies))
        self.AllPossibleCurrencyPairs = CreateAllPossiblePairs(self.CurrencyList)
        self.AllPossibleTransfers = self.__loadFromCacheOrCreateAllPossibleTransfers(preferredPivotCcy)
        self.IsLoaded = False

    def RefreshCache(self):
        for pair, pairValue in self.Markets.items():
            self.RefreshPairTimeSerie(pair )

    def LoadCachedClose(self):
        if self.IsLoaded:
            return
        for pair, pairValue in self.Markets.items():
            data =self.__getCachedPairData(pair, self.Frequency)
            self.CloseDataStorage[str(pair)] = data['Close']
        self.CloseDataStorage =  self.CloseDataStorage.fillna(method='ffill')
        self.CloseDataStorage = self.CloseDataStorage.fillna(method='bfill')
        self.__rebaseToRefCcy()
        self.IsLoaded = True

    def GetTimeSerieClose(self, currency):
        if not self.DisableCalls:
            self.GetSnapshotDataAllMarkets()
        return self.CloseDataStorage[currency]

    def GetCurrentClose(self, currency):
        if not self.DisableCalls:
            self.GetSnapshotDataAllMarkets()
        return self.CloseDataStorage[currency].tail(1)[0]

    def GetTime(self):
        return self.CloseDataStorage.index[-1]

    def RefreshPairTimeSerie(self, currencyPair):
        print("Refreshing " + str(currencyPair) + " " + str(self.Frequency))
        fileName = self.__pairCacheFileName(currencyPair, self.Frequency)
        exchangeData_df = self.Exchange.GetCurrencyPairTimeSerie(currencyPair, self.Frequency)
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

    def __loadFromCacheOrCreateAllPossibleTransfers(self, preferredPivotCcy):
        fileName = os.path.join(self.CacheFolder, "transfers_" + self.ExchangeName + ".json")
        if os.path.isfile(fileName):
            f = open(fileName, 'r')
            data = f.read()
            dataDict = json.loads(data)
            dataDict2 = {CurrencyPair(t): [[u[0], CurrencyPair(u[1])] for u in dataDict[t]] for t in dataDict.keys()}
            currencyPairs = dataDict2.keys()
            extraCurrencyPairs = set(self.AllPossibleCurrencyPairs) - set(currencyPairs)
            if (len(extraCurrencyPairs)) > 0:
                extraTransfers =  CreateAllPossibleTransfers(extraCurrencyPairs, self.Markets, preferredPivotCcy)
                dataDict2.update(extraTransfers)
                self.__dumpTransfersJson(dataDict2, fileName)
            return dataDict2
        else:
            dataDict = CreateAllPossibleTransfers(self.AllPossibleCurrencyPairs, self.Markets, preferredPivotCcy)
            self.__dumpTransfersJson(dataDict, fileName)
            return dataDict

    def __dumpTransfersJson(self, dataDict, fileName):
        dataDictString = {str(t): [[u[0], str(u[1])] for u in dataDict[t]] for t in dataDict.keys()}
        dataJson = json.dumps(dataDictString, sort_keys=True, indent=4, separators=(',', ': '))
        f = open(fileName, 'w')
        f.write(dataJson)



