import urllib.request
import json
import pandas as pd
from Base.ExchangeData import Frequency
from Base.CurrencyPairs import ExchangePair, ExchangePairMarketData, CurrencyPair

BASE_URL = "https://bittrex.com/api/"
GET_MARKETS = "/public/getmarkets"
GET_MARKETS_SUMMARIES = "/public/getmarketsummaries"
GET_PRICES = "/pub/market/GetTicks?marketName="

FREQUENCIES_DICT =  {Frequency.hour: "hour", Frequency.min :"onemin", Frequency.tenmin : "tenmin", Frequency.day:"day"}

class BittrexExchange:

    def __init__(self, apiKey, apiVersion, refCurrency):
        self.ApiKey = apiKey
        self.RefCurrency = refCurrency
        self.ApiVersion = apiVersion
        self.Markets = self.RetrieveMarkets()
        self.Name = "BITTREX"

    def RetrieveLiquidTradedPairs(self, minVolume):
        filteredCurrencies = self.__filterOutCurrencyPairsLowVolume(minVolume)
        return filteredCurrencies

    def RetrieveMarkets(self):
        url = BASE_URL + self.ApiVersion +  GET_MARKETS
        response = urllib.request.urlopen(url)
        data = json.loads(response.read().decode(response.info().get_param('charset') or 'utf-8'))
        processedData = self.__processPairsRequest(data)
        return processedData

    def GetMarketsSnapshot(self):
        pairsDict = self.Markets
        retVal = dict()
        marketsDict = self.__getMarketsSnapshot()
        for pairName, pair in pairsDict.items():
            retVal[pairName] = ExchangePairMarketData(pair, marketsDict[pairName][1],
                                                      marketsDict[pairName][2], marketsDict[pairName][3])
        return retVal

    def GetCurrencyPairTimeSerie(self, currencyPair, frequency):
        ticker = currencyPair.BaseCurrency +"-"+currencyPair.MarketCurrency
        url = BASE_URL + "v2.0" +  GET_PRICES + ticker
        freq = FREQUENCIES_DICT[frequency]
        url = url + "&tickInterval=" + freq
        response = urllib.request.urlopen(url)
        data = json.loads(response.read().decode(response.info().get_param('charset') or 'utf-8'))
        processedData = self.__processTimeSerieData(data)
        return processedData

    def __processTimeSerieData(self, data):
        sucess = data['success']
        if sucess:
            processedData_df = pd.DataFrame(data['result'])
            processedData_df = processedData_df.rename({'O':'Open', 'H':'High','L':'Low','C':'Close','V':'Volume','BV':'BaseVolume'}, axis='columns')
            processedData_df = processedData_df.set_index('T')
            processedData_df.index = pd.to_datetime(processedData_df.index)
            return processedData_df
        else:
            return "Invalid answer"


    def __processPairsRequest(self, data):
        dataDict = data['result']
        return dict((CurrencyPair(t['BaseCurrency'], t['MarketCurrency']), ExchangePair(CurrencyPair(t['BaseCurrency'], t['MarketCurrency']), t['MinTradeSize']))
                    for t in dataDict if t['IsActive'] is True)

    def __filterOutCurrencyPairsLowVolume(self, minVolume):
        pairsDict = self.Markets
        marketsDict = self.__getMarketsSnapshot()
        retVal = dict()
        for pairName, pair in pairsDict.items():
            if pairName in marketsDict:
                volume = marketsDict[pairName][0]
                refPrice = self.__getLastInRefCurrency(marketsDict, pairName.BaseCurrency)
                volumeInRefCurrency = refPrice * volume
                if volumeInRefCurrency > minVolume:
                    retVal[pairName] = ExchangePairMarketData(pair, marketsDict[pairName][1],
                                                              marketsDict[pairName][2],marketsDict[pairName][3])
        return retVal

    def __getMarketsSnapshot(self):
        url = BASE_URL + self.ApiVersion +  GET_MARKETS_SUMMARIES
        response = urllib.request.urlopen(url)
        data = json.loads(response.read().decode(response.info().get_param('charset') or 'utf-8'))
        data = self.__processMarketsSummary(data)
        return data

    def __processMarketsSummary(self, data):
        dataDict = data['result'];
        return dict((CurrencyPair(t['MarketName']), (t['BaseVolume'],t['Last'], t['Bid'], t['Ask']))
                    for t in dataDict)

    def __getLastInRefCurrency(self, marketDict, currency):
        currencyPair = CurrencyPair(self.RefCurrency,currency)
        reversePair = currencyPair.GetReversePair()
        if currencyPair in marketDict:
            return marketDict[currencyPair][1]
        elif reversePair in marketDict:
            return marketDict[reversePair][1]
        elif currency == self.RefCurrency:
            return 1.0
        else:
            return None



