# -*- coding: utf-8 -*-

from Exchange.BittrexExchange import BittrexExchange
from Strategies.TriangularArbitrageur import TriangularArbitrageur
from DataProvider import DataProvider
from Base.ExchangeData import Frequency
from Base.CurrencyPairs import CurrencyPair

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

def CacheTest():
    bittrex = BittrexExchange("asdasdasda2", "v1.1", "USDT")
    currentDir = os.path.abspath(os.path.dirname(__file__))
    cacheFolder = os.path.join(currentDir, "Cache")
    dataProvider = DataProvider(bittrex, cacheFolder)
    startDate = dt.datetime(2017,10,28)
    endDate = dt.datetime(2017,11,30)
    marketPairs = bittrex.RetrieveMarkets()
    t= dataProvider.GetPairTimeSerie(CurrencyPair("BTC-ETH"),Frequency.hour, startDate, endDate)
    dataProvider.RefreshCache(marketPairs, Frequency.min)
    b=2


if __name__ == "__main__":
    CacheTest()