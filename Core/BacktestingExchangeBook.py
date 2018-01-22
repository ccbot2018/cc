from Core.CurrencyPairs import CurrencyPair

class BacktestingExchangeBook:
    def __init__(self, initialBalance, fees, bidAskSpread):
        self.Balance = initialBalance
        self.Orders = list()
        self.BidAskSpread = bidAskSpread
        self.Fees = fees

    def Buy(self, sellCurrency, quantitySellCurrency, buyCurrency, dataProvider):
        if sellCurrency in self.Balance and self.Balance[sellCurrency]>= quantitySellCurrency:
            self.Balance[sellCurrency] -= quantitySellCurrency
            buyCcyPrice = dataProvider.GetCurrentClose(buyCurrency)
            sellCcyPrice = dataProvider.GetCurrentClose(sellCurrency)
            qtyBuyCurrency = (quantitySellCurrency *sellCcyPrice / (buyCcyPrice*(1.0+self.BidAskSpread/2.0)))*(1.0-self.Fees)
            if buyCurrency in self.Balance:
                self.Balance[buyCurrency] += qtyBuyCurrency
            else:
                self.Balance[buyCurrency] = qtyBuyCurrency
            self.Orders.append((dataProvider.GetTime(), sellCurrency, quantitySellCurrency, buyCurrency, qtyBuyCurrency))
            print((sellCurrency, quantitySellCurrency, buyCurrency, qtyBuyCurrency))
        else:
            return False

    def GetBalance(self):
        return {k: v for k, v in self.Balance.items() if v>0}

    def ComputeMtMPerHolding(self, dataProvider):
        retVal = dict()
        for currency, quantity in self.Balance.items():
            currencyPrice = dataProvider.GetCurrentClose(currency)
            retVal[currency] = quantity * currencyPrice
        return retVal

    def ComputeMtM(self, dataProvider):
        mtm = 0
        for currency, quantity in self.Balance.items():
            currencyPrice = dataProvider.GetCurrentClose(currency)
            mtm += quantity * currencyPrice
        return mtm






