from enum import Enum


class BuySell(Enum):
    Buy = 'buy'
    Sell = 'sell'


class ExchangePair:
    def __init__(self, ccy_pair, min_trade_size):
        self.currency_pair = ccy_pair
        self.min_trade_size = min_trade_size

    def __str__(self):
        return str(self.currency_pair)


class ExchangePairMarketData:
    def __init__(self, exchange_pair, last, bid, ask):
        self.currency_pair = exchange_pair.currency_pair
        self.min_trade_size = exchange_pair.min_trade_size
        self.bid = bid
        self.ask = ask
        self.last = last
        self.mid = (bid + ask) / 2

    def __str__(self):
        return str(self.currency_pair)

    def to_string(self):
        return ",".join([str(self.min_trade_size), str(self.ask), str(self.bid), str(self.last), str(self.min_trade_size)])


class CurrencyPair:
    def __init__(self, *args):
        if len(args) == 1:
            self.__initialize_from_market(args[0])
        elif len(args) == 2:
            self.__initialize_from_pair(args[0], args[1])

    def __initialize_from_pair(self, base_currency, market_currency):
        self.base_currency = base_currency
        self.market_currency = market_currency

    def __initialize_from_market(self, market_name):
        if '-' in market_name:
            bits = market_name.split('-')
            self.base_currency = bits[0]
            self.market_currency = bits[1]

    def __key(self):
        return self.base_currency, self.market_currency

    def __eq__(x, y):
        return x.__key() == y.__key()

    def __hash__(self):
        return hash(self.__key())

    def __str__(self):
        return self.base_currency + "-" + self.market_currency

    def __gt__(self, other):
        return str(self) > str(other)

    def __lt__(self, other):
        return str(self) < str(other)

    def get_reverse_pair(self):
        return CurrencyPair(self.market_currency, self.base_currency)
