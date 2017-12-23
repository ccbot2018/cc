# -*- coding: utf-8 -*-

from Exchange.BittrexExchange import BittrexExchange
from Strategies.TriangularArbitrageur import TriangularArbitrageur
from DataProvider import DataProvider
from Core.ExchangeData import Frequency
from Core.CurrencyPairs import CurrencyPair

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
    dataProvider = DataProvider(bittrex, cacheFolder, [Frequency.min],marketPairsFiltered)
    dataProvider.LoadCachedClose()
    t = dataProvider.GetSnapshotDataAllMarkets(True)
    b=2





if __name__ == "__main__":
    CacheSnapshotTest()