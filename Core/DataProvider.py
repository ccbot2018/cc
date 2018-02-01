import os.path
import pandas as pd
import datetime as dt
import numpy as np
import json
import time

from Core.Frequency import Frequency
from Core.CurrencyPairs import CurrencyPair
from Core.ExchangePairsHelper import create_all_possible_pairs, create_all_possible_transfers
from Utilities.FileWriter import write_file, write_binary_file

dateCache = {}

date_format = '%Y-%m-%d %H:%M:%S'


def cached_date_parser(s):
    if s in dateCache:
        return dateCache[s]
    date = pd.to_datetime(s, format=date_format)
    dateCache[s] = date
    return date


def dump_transfers_to_json(data_dict, file_name):
    data_dict_string = {str(t): [[u[0], str(u[1])] for u in data_dict[t]] for t in data_dict.keys()}
    data_json = json.dumps(data_dict_string, sort_keys=True, indent=4, separators=(',', ': '))
    f = open(file_name, 'w')
    f.write(data_json)


class DataProvider:
    def __init__(self, exchange, cache_folder, frequency, pairs_dict):
        self.exchange = exchange
        self.exchange_name = exchange.name
        self.cache_folder = cache_folder
        self.frequency = frequency
        self.markets_dict = pairs_dict
        self.close_data_storage = pd.DataFrame()
        self.open_data_storage = pd.DataFrame()
        self.high_data_storage = pd.DataFrame()
        self.low_data_storage = pd.DataFrame()
        self.volume_data_storage = pd.DataFrame()
        self.disable_calls = False
        self.reference_currency = exchange.reference_currency
        self.to_reference_currency_dict = self.__to_ref_currency_dict()
        currencies, base_currencies = self.__extract_currency_list()
        self.currency_list = set(list(base_currencies) + list(currencies))
        self.all_possible_currency_pairs = create_all_possible_pairs(self.currency_list)
        self.all_possible_transfers = self.__load_from_cache_or_create_all_transfers(
            self.exchange.preferred_pivot_currency)
        self.is_loaded = False

    def refresh_cache(self):
        for pair in sorted(self.markets_dict.keys()):
            self.__refresh_pair_time_serie(pair)

    def load_cache(self):
        if self.is_loaded:
            return
        for pair in sorted(self.markets_dict.keys()):
            data = self.__get_cached_pair_data(pair, self.frequency)
            data = data[~data.index.duplicated(keep='first')]
            if not data.empty:
                self.close_data_storage[str(pair)] = data['Close']
                self.open_data_storage[str(pair)] = data['Open']
                self.high_data_storage[str(pair)] = data['High']
                self.low_data_storage[str(pair)] = data['Low']
                self.volume_data_storage[str(pair)] = data['Volume']
        self.close_data_storage = self.close_data_storage.fillna(method='ffill')
        self.close_data_storage = self.close_data_storage.fillna(method='bfill')
        self.open_data_storage = self.open_data_storage.fillna(method='ffill')
        self.open_data_storage = self.open_data_storage.fillna(method='bfill')
        self.high_data_storage = self.high_data_storage.fillna(method='ffill')
        self.high_data_storage = self.high_data_storage.fillna(method='bfill')
        self.low_data_storage = self.low_data_storage.fillna(method='ffill')
        self.low_data_storage = self.low_data_storage.fillna(method='bfill')
        self.volume_data_storage = self.volume_data_storage.fillna(method='ffill')
        self.volume_data_storage = self.volume_data_storage.fillna(method='bfill')
        self.__rebase_to_ref_ccy()
        self.is_loaded = True

    def get_time_series_close(self, currency):
        if not self.disable_calls:
            self.get_snapshot_data_all_markets()
        return self.close_data_storage[currency]

    def get_current_close(self, currency):
        if not self.disable_calls:
            self.get_snapshot_data_all_markets()
        return self.close_data_storage[currency].tail(1)[0]

    def get_all_time_series_close(self):
        return self.close_data_storage

    def get_time(self):
        return self.close_data_storage.index[-1]

    def get_currency_list(self):
        return self.reference_currency.keys()

    def __refresh_pair_time_serie(self, currency_pair):
        print("Refreshing " + str(currency_pair) + " " + str(self.frequency))
        file_name = self.__pair_cache_file_name(currency_pair, self.frequency)
        exchange_data_df = self.exchange.get_currency_pair_time_serie(currency_pair, self.frequency)
        print("Retrieved data")
        cache_data_df = pd.DataFrame()
        if os.path.isfile(file_name):
            cache_data_df = pd.read_csv(file_name, index_col=0,
                                        header=None, names=["BaseVolume", "Close", "High", "Low", "Open", "Volume"],
                                        parse_dates=True)
        if not cache_data_df.empty:
            last_cache_date = cache_data_df.index.values[-1]
            exchange_data_df = exchange_data_df.loc[exchange_data_df.index > last_cache_date]
            with open(file_name, 'a') as f:
                exchange_data_df.to_csv(f, header=False)
        else:
            with open(file_name, 'w') as f:
                exchange_data_df.to_csv(f, header=False)

    def get_snapshot_data_all_markets(self):
        all_markets = self.exchange.get_markets_snapshot()
        stamp_date = dt.datetime.now()
        empty_row = np.empty((1, len(self.close_data_storage.columns)))
        empty_row[:] = np.nan
        empty_row_df = pd.DataFrame(empty_row, columns=self.close_data_storage.columns)
        empty_row_df = empty_row_df.set_index(pd.DatetimeIndex([stamp_date]))
        self.close_data_storage = self.close_data_storage.append(empty_row_df)
        for pair in self.markets_dict:
            self.__cache_snapshot_pair(pair, all_markets[pair], stamp_date)
        self.__rebase_to_ref_ccy()

    def output_prices_all_to_csv(self):
        currencies = [t for t in list(self.close_data_storage) if '-' not in t]
        write_file(self.cache_folder, self.exchange_name + "_Close", self.close_data_storage[currencies], True)
        write_file(self.cache_folder, self.exchange_name + "_Open", self.open_data_storage[currencies], True)
        write_file(self.cache_folder, self.exchange_name + "_High", self.high_data_storage[currencies], True)
        write_file(self.cache_folder, self.exchange_name + "_Low", self.low_data_storage[currencies], True)
        write_file(self.cache_folder, self.exchange_name + "_Volume", self.volume_data_storage, True)

    def output_ptices_for_given_currency_list_to_csv(self, currencies):
        write_file(self.cache_folder, self.exchange_name + "_Close", self.close_data_storage[currencies], True)
        write_file(self.cache_folder, self.exchange_name + "_Open", self.open_data_storage[currencies], True)
        write_file(self.cache_folder, self.exchange_name + "_High", self.high_data_storage[currencies], True)
        write_file(self.cache_folder, self.exchange_name + "_Low", self.low_data_storage[currencies], True)
        write_file(self.cache_folder, self.exchange_name + "_Volume", self.volume_data_storage, True)

    def write_binary_cache(self):
        close_data_rec_array = self.__process_table_to_be_cached(self.close_data_storage)
        open_data_rec_array = self.__process_table_to_be_cached(self.open_data_storage)
        low_data_rec_array = self.__process_table_to_be_cached(self.low_data_storage)
        high_data_rec_array = self.__process_table_to_be_cached(self.high_data_storage)
        volume_data_rec_array = self.__process_table_to_be_cached(self.volume_data_storage)
        write_binary_file(self.cache_folder, self.exchange_name + "_Close", close_data_rec_array, False)
        write_binary_file(self.cache_folder, self.exchange_name + "_Open", open_data_rec_array, False)
        write_binary_file(self.cache_folder, self.exchange_name + "_High", high_data_rec_array, False)
        write_binary_file(self.cache_folder, self.exchange_name + "_Low", low_data_rec_array, False)
        write_binary_file(self.cache_folder, self.exchange_name + "_Volume", volume_data_rec_array, False)

    def load_binary_cache(self):
        close_file_path =  os.path.join(self.cache_folder, self.exchange_name + "_Close.bin")
        df = np.fromfile(close_file_path)
        a = 2

    @staticmethod
    def __process_table_to_be_cached(data_table):
        data_table2 = pd.DataFrame(data_table)
        date_time_index = pd.to_datetime(data_table.index.values)
        string_dates = [t.strftime("%Y-%m-%d %H:%M:%S") for t in date_time_index]
        rec_array = data_table2.to_records()
        rec_array["index"] = string_dates
        dtypes = rec_array.dtype.descr
        dtypes[0] = ('index', 'U')
        new_rec_array = rec_array.astype(dtypes)
        return new_rec_array

    def __get_cached_pair_data(self, currency_pair, frequency):
        print("Retrieving " + str(currency_pair) + " " + str(frequency))
        start_time = time.clock()
        file_name = self.__pair_cache_file_name(currency_pair, frequency)
        cache_data_df = pd.DataFrame()
        if frequency is not Frequency.snapshot:
            if os.path.isfile(file_name):
                cache_data_df = pd.read_csv(file_name, index_col=0,
                                            header=None, names=["BaseVolume", "Close", "High", "Low", "Open", "Volume"],
                                            parse_dates=True, date_parser=cached_date_parser)
        else:
            cache_data_df = pd.read_csv(file_name, index_col=0,
                                        header=None, names=["MinTradeSize", "Ask", "Bid", "Close", "Mid"],
                                        parse_dates=True)
        print(str(time.clock() - start_time))
        return cache_data_df

    def __cache_snapshot_pair(self, currency_pair, exchange_pair_market_data, stamp_date):
        file_name = self.__pair_cache_file_name(currency_pair, Frequency.snapshot)
        self.close_data_storage[str(currency_pair)][-1] = exchange_pair_market_data.Last
        self.close_data_storage[str(currency_pair)] = self.close_data_storage[str(currency_pair)].fillna(method='ffill')
        cache_data_df = pd.DataFrame()
        if os.path.isfile(file_name):
            cache_data_df = pd.read_csv(file_name, index_col=0,
                                        header=None, names=["MinTradeSize", "Ask", "Bid", "Last", "Mid"],
                                        parse_dates=True)
        if not cache_data_df.empty:
            with open(file_name, 'a') as f:
                f.writelines(
                    [stamp_date.strftime(date_format) + "," + exchange_pair_market_data.to_string() + "\n"])
        else:
            with open(file_name, 'w') as f:
                f.writelines(
                    [stamp_date.strftime(date_format) + "," + exchange_pair_market_data.to_string() + "\n"])

    def __pair_cache_file_name(self, ticker, frequency):
        return os.path.join(self.cache_folder, self.exchange_name, str(ticker) + "_" + frequency.value + ".csv")

    def __rebase_to_ref_ccy(self):
        for ccy in sorted(self.to_reference_currency_dict.keys()):
            ccy_path = self.to_reference_currency_dict[ccy]
            self.close_data_storage[ccy] = 1.0
            self.open_data_storage[ccy] = 1.0
            self.high_data_storage[ccy] = 1.0
            self.low_data_storage[ccy] = 1.0
            for pair in ccy_path:
                if pair[0] == "Long":
                    self.close_data_storage[ccy] *= self.close_data_storage[str(pair[1])]
                    self.open_data_storage[ccy] *= self.open_data_storage[str(pair[1])]
                    self.high_data_storage[ccy] *= self.high_data_storage[str(pair[1])]
                    self.low_data_storage[ccy] *= self.low_data_storage[str(pair[1])]
                elif pair[0] == "Short":
                    self.close_data_storage[ccy] *= 1.0 / self.close_data_storage[str(pair[1])]
                    self.open_data_storage[ccy] *= 1.0 / self.open_data_storage[str(pair[1])]
                    self.high_data_storage[ccy] *= 1.0 / self.high_data_storage[str(pair[1])]
                    self.low_data_storage[ccy] *= 1.0 / self.low_data_storage[str(pair[1])]
        for pair, pairValue in self.markets_dict.items():
            market_currency_price = self.close_data_storage[pair.market_currency]
            self.volume_data_storage[str(pair)] *= market_currency_price

    def __extract_currency_list(self):
        currencies = set()
        base_currencies = set()
        for pairName, pair in self.markets_dict.items():
            if pairName.base_currency not in currencies:
                currencies.add(pairName.base_currency)
            if pairName.base_currency not in base_currencies:
                base_currencies.add(pairName.base_currency)
            if pairName.market_currency not in currencies:
                currencies.add(pairName.market_currency)
        return currencies, base_currencies

    def __to_ref_currency_dict(self):
        ret_val = dict()
        currencies, base_currencies = self.__extract_currency_list()
        for base_currency in base_currencies:
            ccy_pair = CurrencyPair(self.reference_currency, base_currency)
            if ccy_pair in self.markets_dict:
                ret_val[base_currency] = list([("Long", ccy_pair)])
            elif ccy_pair.get_reverse_pair() in self.markets_dict:
                ret_val[base_currency] = list([("Short", ccy_pair.get_reverse_pair())])
            elif ccy_pair.base_currency == self.reference_currency:
                ret_val[base_currency] = list(["Identity"])
        for currency in currencies:
            for base_currency in base_currencies:
                base_pair = CurrencyPair(base_currency, currency)
                if base_pair in self.markets_dict:
                    pivot_to_ref = ret_val[base_currency][0]
                    ret_val[currency] = list([("Long", base_pair)])
                    ret_val[currency].append(pivot_to_ref)
        return ret_val

    def __load_from_cache_or_create_all_transfers(self, preferred_pivot_ccy):
        file_name = os.path.join(self.cache_folder, "transfers_" + self.exchange_name + ".json")
        if os.path.isfile(file_name):
            f = open(file_name, 'r')
            data = f.read()
            data_dict = json.loads(data)
            data_dict2 = {CurrencyPair(t): [[u[0], CurrencyPair(u[1])] for u in data_dict[t]] for t in data_dict.keys()}
            currency_pairs = data_dict2.keys()
            extra_currency_pairs = set(self.all_possible_currency_pairs) - set(currency_pairs)
            if (len(extra_currency_pairs)) > 0:
                extra_transfers = create_all_possible_transfers(extra_currency_pairs, self.markets_dict, preferred_pivot_ccy)
                data_dict2.update(extra_transfers)
                dump_transfers_to_json(data_dict2, file_name)
            return data_dict2
        else:
            data_dict = create_all_possible_transfers(self.all_possible_currency_pairs, self.markets_dict, preferred_pivot_ccy)
            dump_transfers_to_json(data_dict, file_name)
            return data_dict
