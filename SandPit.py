# -*- coding: utf-8 -*-

from Exchange.BittrexExchange import BittrexExchange
from Strategies.TriangularArbitrageur import TriangularArbitrageur
from Core.DataProvider import DataProvider
from Core.ExchangeData import Frequency
from Strategies.BuyAndHold import BuyAndHold
from Core.BackTestingMonoExchange import BackTestingMonoExchange

import datetime as dt
import os.path

def TriangularArbitrage():
    bittrex = BittrexExchange("asdasdasda2", "v1.1","USDT")
    pairs = bittrex.RetrieveLiquidTradedPairs(50000)
    arbitrageur = TriangularArbitrageur(pairs, 0.0025)
    while(True):
        a = dt.datetime.now()
        data = bittrex.GetMarketsSnapshot()
        b= dt.datetime.now()
        arbitrageur.EvaluateArbitragePossibilities(data)
        c = dt.datetime.now()
        print(str(a) +"  " + str((b-a).microseconds/1000) + "   " + str((c-b).microseconds/1000))

def RefreshCache():
    bittrex = BittrexExchange("asdasdasda2", "v1.1", "USDT")
    currentDir = os.path.abspath(os.path.dirname(__file__))
    cacheFolder = os.path.join(currentDir, "Cache")

    marketPairs = bittrex.RetrieveMarkets()
    dataProvider = DataProvider(bittrex, marketPairs)
    dataProvider.RefreshCache(Frequency.fivemin)
    dataProvider.RefreshCache(Frequency.thirtymin)
    dataProvider.RefreshCache(Frequency.hour)
    dataProvider.RefreshCache(Frequency.min)

def CacheSnapshotTest():
    bittrex = BittrexExchange("asdasdasda2", "v1.1", "USDT")
    currentDir = os.path.abspath(os.path.dirname(__file__))
    cacheFolder = os.path.join(currentDir, "Cache")

    marketPairs = bittrex.RetrieveMarkets()
    btcMarkets = list(filter(lambda t: t.BaseCurrency =="BTC" or t.MarketCurrency == "BTC",marketPairs.keys()))
    marketPairsFiltered = {key: marketPairs[key] for key in btcMarkets}
    dataProvider = DataProvider(bittrex, cacheFolder, Frequency.min,marketPairsFiltered, "USDT")
    dataProvider.LoadCachedClose()
    t = dataProvider.GetSnapshotDataAllMarkets()
    b=2

def SimpleStrategyBacktesting():
    startDate = dt.datetime(2017,12,12)
    endDate = dt.datetime(2017, 12, 22)
    strategy = BuyAndHold("BTC")
    bittrex = BittrexExchange("asdasdasda2", "v1.1", "USDT")
    currentDir = os.path.abspath(os.path.dirname(__file__))
    cacheFolder = os.path.join(currentDir, "Cache")
    marketPairs = bittrex.RetrieveMarkets()
    btcMarkets = list(filter(lambda t: t.BaseCurrency == "BTC" or t.MarketCurrency == "BTC", marketPairs.keys()))
    marketPairsFiltered = {key: marketPairs[key] for key in btcMarkets}
    dataProvider = DataProvider(bittrex, cacheFolder, Frequency.thirtymin, marketPairsFiltered, "USDT")
    bc = BackTestingMonoExchange(startDate, endDate, Frequency.thirtymin, strategy,dataProvider, 0.0025, 0.01)
    bc.SetUp()
    mtm = bc.Start()
    mtm = map(lambda t: str(t) +"\n", mtm)
    with open(cacheFolder + "\\result.csv", 'w') as f:
        f.writelines(mtm)



if __name__ == "__main__":
    SimpleStrategyBacktesting()