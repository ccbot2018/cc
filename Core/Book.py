from Core.CurrencyPairs import CurrencyPair

class ExchangeBook:
    def __init__(self, initialBalance, fees, bidAskSpread):
        self.Balance = initialBalance
        self.Orders = list()
        self.BidAskSpread = bidAskSpread
        self.Fees = fees

    def Buy(self, sellCurrency, quantitySellCurrency, buyCurrency, dataProvider):
        if sellCurrency in self.Balance and self.Balance[sellCurrency]>= quantitySellCurrency:
            self.Balance[sellCurrency] -= quantitySellCurrency
            buyCcyPrice = dataProvider.GetCurrentClose(buyCurrency)
            qtyBuyCurrency = (quantitySellCurrency / (buyCcyPrice*(1.0+self.BidAskSpread)))*(1.0-self.Fees)
            if buyCurrency in self.Balance:
                self.Balance[buyCurrency] += qtyBuyCurrency
            else:
                self.Balance[buyCurrency] = qtyBuyCurrency
            self.Orders.append((sellCurrency, quantitySellCurrency, buyCurrency,qtyBuyCurrency))
            print((sellCurrency, quantitySellCurrency, buyCurrency,qtyBuyCurrency))
        else:
            return False

    def GetBalance(self):
        return {k: v for k, v in self.Balance.items() if v>0}

    def ComputeMtM(self, dataProvider):
        mtm = 0
        for currency, quantity in self.Balance.items():
            currencyPrice = dataProvider.GetCurrentClose(currency)
            mtm += quantity * currencyPrice
        return mtm






