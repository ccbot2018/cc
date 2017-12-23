from enum import Enum

class BuySell(Enum):
    Buy = 'buy'
    Sell = 'sell'

class ExchangePair:
    def __init__(self, ccyPair, minTradeSize):
        self.CurrencyPair = ccyPair
        self.MinTradeSize = minTradeSize

    def __str__(self):
        return str(self.CurrencyPair)


class ExchangePairMarketData:
    def __init__(self, exchangePair, last, bid, ask):
        self.CurrencyPair = exchangePair.CurrencyPair
        self.MinTradeSize = exchangePair.MinTradeSize
        self.Bid = bid
        self.Ask = ask
        self.Last = last
        self.Mid = (bid+ask)/2

    def __str__(self):
        return str(self.CurrencyPair)

    def ToString(self):
        return ",".join([str(self.MinTradeSize), str(self.Ask), str(self.Bid), str(self.Last), str(self.Mid)])

class CurrencyPair:
    def __init__(self, *args):
        if len(args) == 1:
            self.__initializeFromMarket(args[0])
        elif len(args) == 2:
            self.__initializeFromPair(args[0], args[1])

    def __initializeFromPair(self, baseCurrency, marketCurrency):
        self.BaseCurrency = baseCurrency
        self.MarketCurrency = marketCurrency

    def __initializeFromMarket(self, marketName):
        if '-' in marketName:
            bits = marketName.split('-')
            self.BaseCurrency = bits[0]
            self.MarketCurrency = bits[1]

    def __key(self):
        return (self.BaseCurrency, self.MarketCurrency)

    def __eq__(x, y):
        return x.__key() == y.__key()

    def __hash__(self):
        return hash(self.__key())

    def __str__(self):
        return self.BaseCurrency +"-"+self.MarketCurrency

    def GetReversePair(self):
        return CurrencyPair(self.MarketCurrency, self.BaseCurrency)