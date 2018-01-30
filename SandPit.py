# -*- coding: utf-8 -*-

from Exchange.BittrexExchange import BittrexExchange
from Strategies.TriangularArbitrageur import TriangularArbitrageur
from Core.DataProvider import DataProvider
from Core.Frequency import Frequency
from Strategies.BuyAndHold import BuyAndHold
from Strategies.BuyBasketAndHold import BuyBasketAndHold
from Core.BacktestingMonoExchange import BackTestingMonoExchange
from Analytics.ReturnsAnalysis import ScatterPlotReturnsExcessReturn
from Strategies.BestPerformersStrategy import BestPerformersStrategy
from Strategies.GenericBalancingStrategy import GenericBalancingStrategy

import datetime as dt
import os.path
import pandas as pd

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
    bittrex = BittrexExchange("asdasdasda2", "v1.1", "USDT", "BTC")
    currentDir = os.path.abspath(os.path.dirname(__file__))
    cacheFolder = os.path.join(currentDir, "Cache")

    marketPairs = bittrex.RetrieveMarkets()
    dataProvider = DataProvider(bittrex, cacheFolder, Frequency.min, marketPairs)
    dataProvider.RefreshCache()
    dataProvider1 = DataProvider(bittrex, cacheFolder, Frequency.fivemin, marketPairs)
    dataProvider1.RefreshCache()
    dataProvider2 = DataProvider(bittrex, cacheFolder, Frequency.thirtymin, marketPairs)
    dataProvider2.RefreshCache()
    dataProvider3 = DataProvider(bittrex, cacheFolder, Frequency.hour, marketPairs)
    dataProvider3.RefreshCache()
    dataProvider4 = DataProvider(bittrex, cacheFolder, Frequency.day, marketPairs)
    dataProvider4.RefreshCache()

def CacheSnapshotTest():
    bittrex = BittrexExchange("asdasdasda2", "v1.1", "USDT", "BTC")
    currentDir = os.path.abspath(os.path.dirname(__file__))
    cacheFolder = os.path.join(currentDir, "Cache")

    marketPairs = bittrex.RetrieveMarkets()
    #btcMarkets = list(filter(lambda t: t.BaseCurrency =="BTC" or t.MarketCurrency == "BTC",marketPairs.keys()))
    #marketPairsFiltered = {key: marketPairs[key] for key in btcMarkets}
    dataProvider = DataProvider(bittrex, cacheFolder, Frequency.min,marketPairs)
    dataProvider.LoadCache()
    dataProvider.OutputCache()
    b=2

def SimpleStrategyBacktesting():
    startDate = dt.datetime(2017,12,12)
    endDate = dt.datetime(2017, 12, 22)
    strategy = BuyBasketAndHold(["ADA", "ETH", "BTC"])
    strategy1 = BuyAndHold("BTC")
    bittrex = BittrexExchange("asdasdasda2", "v1.1", "USDT")
    currentDir = os.path.abspath(os.path.dirname(__file__))
    cacheFolder = os.path.join(currentDir, "Cache")
    marketPairs = bittrex.Markets
    dataProvider = DataProvider(bittrex, cacheFolder, Frequency.thirtymin, marketPairs)
    bc = BackTestingMonoExchange(startDate, endDate, Frequency.thirtymin, strategy,dataProvider, 0.0025, 0.01)
    bc.SetUp()
    mtm = bc.Start()
    mtm = mtm.resample('D').last()
    with open(cacheFolder + "\\result.csv", 'w') as f:
        mtm.to_csv(f, header= False)
    bc = BackTestingMonoExchange(startDate, endDate, Frequency.thirtymin, strategy1,dataProvider, 0.0025, 0.01)
    bc.SetUp()
    mtm = bc.Start()
    mtm = mtm.resample('D').last()
    with open(cacheFolder + "\\result1.csv", 'w') as f:
        mtm.to_csv(f, header= False)

def MomentumStrategy():
    startDate = dt.datetime(2017, 12, 12)
    endDate = dt.datetime(2018, 1, 1)
    currencyPicker = BestPerformersStrategy(2, 24)
    strategy1 = GenericBalancingStrategy(dt.timedelta(0,7200), dt.timedelta(0,300), currencyPicker, 0.05, "BTC")
    bittrex = BittrexExchange("asdasdasda2", "v1.1", "USDT", "BTC")
    currentDir = os.path.abspath(os.path.dirname(__file__))
    cacheFolder = os.path.join(currentDir, "Cache")
    marketPairs = bittrex.Markets
    dataProvider = DataProvider(bittrex, cacheFolder, Frequency.fivemin, marketPairs)
    bc = BackTestingMonoExchange(startDate, endDate, Frequency.fivemin, strategy1, dataProvider, 0.0025, 0.01)
    bc.SetUp()
    mtm = bc.Start()
    mtm = mtm.resample('D').last()
    with open(cacheFolder + "\\resultMomentum.csv", 'w') as f:
        mtm.to_csv(f, header=False)

def ReturnsAnalysis():
    bittrex = BittrexExchange("asdasdasda2", "v1.1", "USDT", "BTC")
    currentDir = os.path.abspath(os.path.dirname(__file__))
    cacheFolder = os.path.join(currentDir, "Cache")
    marketPairs = bittrex.RetrieveMarkets()
    btcMarkets = list(filter(lambda t: t.BaseCurrency == "BTC" or t.MarketCurrency == "BTC", marketPairs.keys()))
    marketPairsFiltered = {key: marketPairs[key] for key in btcMarkets}
    dataProvider = DataProvider(bittrex, cacheFolder, Frequency.min, marketPairsFiltered)
    ScatterPlotReturnsExcessReturn(dataProvider,10,50)



if __name__ == "__main__":
    CacheSnapshotTest()