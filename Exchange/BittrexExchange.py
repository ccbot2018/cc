import urllib.request
import json
import pandas as pd
from Core.Frequency import Frequency
from Core.CurrencyPairs import ExchangePair, ExchangePairMarketData, CurrencyPair

BASE_URL = "https://bittrex.com/api/"
GET_MARKETS = "/public/getmarkets"
GET_MARKETS_SUMMARIES = "/public/getmarketsummaries"
GET_PRICES = "/pub/market/GetTicks?marketName="
GET_MARKET = "/public/getmarketsummary?market="

FREQUENCIES_DICT = {Frequency.hour: "hour", Frequency.min: "onemin", Frequency.thirtymin: "thirtymin",
                    Frequency.day: "day", Frequency.fivemin: "fivemin"}


class BittrexExchange:

    def __init__(self, api_key, api_version, ref_currency, preferred_pivot_currency):
        self.api_key = api_key
        self.reference_currency = ref_currency
        self.api_version = api_version
        self.markets_dict = self.retrieve_markets()
        self.name = "BITTREX"
        self.preferred_pivot_currency = preferred_pivot_currency

    def retrieve_liquid_traded_pairs(self, min_volume):
        filtered_currencies = self.__filter_out_currency_pairs_low_volume(min_volume)
        return filtered_currencies

    def retrieve_markets(self):
        url = BASE_URL + self.api_version + GET_MARKETS
        response = urllib.request.urlopen(url)
        data = json.loads(response.read().decode(response.info().get_param('charset') or 'utf-8'))
        processed_data = self.__process_pairs_request(data)
        return processed_data

    def get_markets_snapshot(self):
        pairs_dict = self.markets_dict
        ret_val = dict()
        markets_dict = self.__get_market_snapshot()
        for pairName, pair in pairs_dict.items():
            ret_val[pairName] = ExchangePairMarketData(pair, markets_dict[pairName][1],
                                                       markets_dict[pairName][2], markets_dict[pairName][3])
        return ret_val

    def get_market_snapshot(self, currency_pair):
        if currency_pair in self.markets_dict:
            url = BASE_URL + self.api_version + GET_MARKET + str(currency_pair)
            response = urllib.request.urlopen(url)
            data = json.loads(response.read().decode(response.info().get_param('charset') or 'utf-8'))
            processed_data = self.__process_market_snapshot_request(data, self.markets_dict[currency_pair], False)
            return processed_data
        elif currency_pair.get_reverse_pair() in self.markets_dict:
            url = BASE_URL + self.api_version + GET_MARKET + str(currency_pair.get_reverse_pair())
            response = urllib.request.urlopen(url)
            data = json.loads(response.read().decode(response.info().get_param('charset') or 'utf-8'))
            processed_data = self.__process_market_snapshot_request(data, self.markets_dict[currency_pair], True)
            return processed_data

    @staticmethod
    def __process_market_snapshot_request(data, exchange_pair, is_reverse):
        success = data['success']
        if success:
            result = data['result'][0]
            if not is_reverse:
                exchange_pair_market_data = ExchangePairMarketData(exchange_pair, result['Last'], result['Bid'],
                                                                   result['Ask'])
            else:
                exchange_pair_market_data = ExchangePairMarketData(exchange_pair, 1.0 / result['Last'],
                                                                   1.0 / result['Ask'],
                                                                   1.0 / result['Bid'])
            return exchange_pair_market_data
        else:
            return "Invalid answer"

    def get_currency_pair_time_serie(self, currency_pair, frequency):
        ticker = currency_pair.base_currency + "-" + currency_pair.market_currency
        url = BASE_URL + "v2.0" + GET_PRICES + ticker
        freq = FREQUENCIES_DICT[frequency]
        url = url + "&tickInterval=" + freq
        response = urllib.request.urlopen(url)
        data = json.loads(response.read().decode(response.info().get_param('charset') or 'utf-8'))
        processed_data = self.__process_time_serie_data(data)
        return processed_data

    @staticmethod
    def __process_time_serie_data(data):
        success = data['success']
        if success:
            print(success)
            processed_data_df = pd.DataFrame(data['result'])
            processed_data_df = processed_data_df.rename(
                {'O': 'Open', 'H': 'High', 'L': 'Low', 'C': 'Close', 'V': 'Volume', 'BV': 'BaseVolume'}, axis='columns')
            processed_data_df = processed_data_df.set_index('T')
            processed_data_df.index = pd.to_datetime(processed_data_df.index)
            return processed_data_df
        else:
            return "Invalid answer"

    @staticmethod
    def __process_pairs_request(data):
        data_dict = data['result']
        return dict((CurrencyPair(t['BaseCurrency'], t['MarketCurrency']),
                     ExchangePair(CurrencyPair(t['BaseCurrency'], t['MarketCurrency']), t['MinTradeSize']))
                    for t in data_dict if t['IsActive'] is True)

    def __filter_out_currency_pairs_low_volume(self, min_volume):
        pairs_dict = self.markets_dict
        markets_data_dict = self.__get_market_snapshot()
        ret_val = dict()
        for pairName, pair in pairs_dict.items():
            if pairName in markets_data_dict:
                volume = markets_data_dict[pairName][0]
                ref_price = self.__get_last_in_ref_currency(markets_data_dict, pairName.base_currency)
                volume_in_ref_currency = ref_price * volume
                if volume_in_ref_currency > min_volume:
                    ret_val[pairName] = ExchangePairMarketData(pair, markets_data_dict[pairName][1],
                                                               markets_data_dict[pairName][2], markets_data_dict[pairName][3])
        return ret_val

    def __get_market_snapshot(self):
        url = BASE_URL + self.api_version + GET_MARKETS_SUMMARIES
        response = urllib.request.urlopen(url)
        data = json.loads(response.read().decode(response.info().get_param('charset') or 'utf-8'))
        data = self.__process_markets_summary(data)
        return data

    @staticmethod
    def __process_markets_summary(data):
        data_dict = data['result']
        return dict((CurrencyPair(t['MarketName']), (t['BaseVolume'], t['Last'], t['Bid'], t['Ask']))
                    for t in data_dict)

    def __get_last_in_ref_currency(self, market_dict, currency):
        currency_pair = CurrencyPair(self.reference_currency, currency)
        reverse_pair = currency_pair.get_reverse_pair()
        if currency_pair in market_dict:
            return market_dict[currency_pair][1]
        elif reverse_pair in market_dict:
            return market_dict[reverse_pair][1]
        elif currency == self.reference_currency:
            return 1.0
        else:
            return None
