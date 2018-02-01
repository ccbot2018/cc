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


def triangular_arbitrage():
    bittrex = BittrexExchange("asdasdasda2", "v1.1", "USDT")
    pairs = bittrex.retrieve_liquid_traded_pairs(50000)
    arbitrageur = TriangularArbitrageur(pairs, 0.0025)
    while True:
        a = dt.datetime.now()
        data = bittrex.get_markets_snapshot()
        b = dt.datetime.now()
        arbitrageur.EvaluateArbitragePossibilities(data)
        c = dt.datetime.now()
        print(str(a) + "  " + str((b - a).microseconds / 1000) + "   " + str((c - b).microseconds / 1000))


def refresh_cache():
    bittrex = BittrexExchange("asdasdasda2", "v1.1", "USDT", "BTC")
    current_dir = os.path.abspath(os.path.dirname(__file__))
    cache_folder = os.path.join(current_dir, "Cache")

    market_pairs = bittrex.retrieve_markets()
    data_provider = DataProvider(bittrex, cache_folder, Frequency.min, market_pairs)
    data_provider.refresh_cache()
    data_provider1 = DataProvider(bittrex, cache_folder, Frequency.fivemin, market_pairs)
    data_provider1.refresh_cache()
    data_provider2 = DataProvider(bittrex, cache_folder, Frequency.thirtymin, market_pairs)
    data_provider2.refresh_cache()
    data_provider3 = DataProvider(bittrex, cache_folder, Frequency.hour, market_pairs)
    data_provider3.refresh_cache()
    data_provider4 = DataProvider(bittrex, cache_folder, Frequency.day, market_pairs)
    data_provider4.refresh_cache()


def cache_snapshot_test():
    bittrex = BittrexExchange("asdasdasda2", "v1.1", "USDT", "BTC")
    current_dir = os.path.abspath(os.path.dirname(__file__))
    cache_folder = os.path.join(current_dir, "Cache")

    market_pairs = bittrex.retrieve_markets()
    # btcMarkets = list(filter(lambda t: t.BaseCurrency =="BTC" or t.MarketCurrency == "BTC",marketPairs.keys()))
    # marketPairsFiltered = {key: marketPairs[key] for key in btcMarkets}
    market_pairs = {k: market_pairs[k] for k in list(market_pairs.keys())[:10]}
    data_provider = DataProvider(bittrex, cache_folder, Frequency.min, market_pairs)
    #data_provider.load_binary_cache()
    data_provider.load_cache()
    data_provider.write_binary_cache()
    b = 2


def simple_strategy_backtesting():
    start_date = dt.datetime(2017, 12, 12)
    end_date = dt.datetime(2017, 12, 22)
    strategy = BuyBasketAndHold(["ADA", "ETH", "BTC"])
    strategy1 = BuyAndHold("BTC")
    bittrex = BittrexExchange("asdasdasda2", "v1.1", "USDT")
    current_dir = os.path.abspath(os.path.dirname(__file__))
    cache_folder = os.path.join(current_dir, "Cache")
    market_pairs = bittrex.markets_dict
    data_provider = DataProvider(bittrex, cache_folder, Frequency.thirtymin, market_pairs)
    bc = BackTestingMonoExchange(start_date, end_date, Frequency.thirtymin, strategy, data_provider, 0.0025, 0.01)
    bc.SetUp()
    mtm = bc.Start()
    mtm = mtm.resample('D').last()
    with open(cache_folder + "\\result.csv", 'w') as f:
        mtm.to_csv(f, header=False)
    bc = BackTestingMonoExchange(start_date, end_date, Frequency.thirtymin, strategy1, data_provider, 0.0025, 0.01)
    bc.SetUp()
    mtm = bc.Start()
    mtm = mtm.resample('D').last()
    with open(cache_folder + "\\result1.csv", 'w') as f:
        mtm.to_csv(f, header=False)


def momentum_strategy():
    start_date = dt.datetime(2017, 12, 12)
    end_date = dt.datetime(2018, 1, 1)
    currency_picker = BestPerformersStrategy(2, 24)
    strategy1 = GenericBalancingStrategy(dt.timedelta(0, 7200), dt.timedelta(0, 300), currency_picker, 0.05, "BTC")
    bittrex = BittrexExchange("asdasdasda2", "v1.1", "USDT", "BTC")
    current_dir = os.path.abspath(os.path.dirname(__file__))
    cache_folder = os.path.join(current_dir, "Cache")
    market_pairs = bittrex.markets_dict
    data_provider = DataProvider(bittrex, cache_folder, Frequency.fivemin, market_pairs)
    bc = BackTestingMonoExchange(start_date, end_date, Frequency.fivemin, strategy1, data_provider, 0.0025, 0.01)
    bc.SetUp()
    mtm = bc.Start()
    mtm = mtm.resample('D').last()
    with open(cache_folder + "\\resultMomentum.csv", 'w') as f:
        mtm.to_csv(f, header=False)


def return_analysis():
    bittrex = BittrexExchange("asdasdasda2", "v1.1", "USDT", "BTC")
    current_dir = os.path.abspath(os.path.dirname(__file__))
    cache_folder = os.path.join(current_dir, "Cache")
    market_pairs = bittrex.retrieve_markets()
    btc_markets = list(filter(lambda t: t.BaseCurrency == "BTC" or t.MarketCurrency == "BTC", market_pairs.keys()))
    market_pairs_filtered = {key: market_pairs[key] for key in btc_markets}
    data_provider = DataProvider(bittrex, cache_folder, Frequency.min, market_pairs_filtered)
    ScatterPlotReturnsExcessReturn(data_provider, 10, 50)


if __name__ == "__main__":
    cache_snapshot_test()
