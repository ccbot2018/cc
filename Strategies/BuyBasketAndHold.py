from Core.CurrencyPairs import CurrencyPair

class BuyBasketAndHold:
    def __init__(self, currencyList):
        self.Book = None
        self.DataProvider = None
        self.State = "NoPosition"
        self.CurrencyList = currencyList

    def Update(self, date):
        if self.State == "NoPosition":
            balance = self.Book.get_balance()
            nBuyCurrency = len(self.CurrencyList)
            for ccy, qty in balance.items():
                for buyCcy in self.CurrencyList:
                    self.MultipleBuys(ccy, qty/nBuyCurrency, buyCcy)
            self.State = "Holding"
            return self.Book.compute_mtm(self.DataProvider)
        elif self.State =="Holding":
            return self.Book.compute_mtm(self.DataProvider)

    def MultipleBuys(self, sellCcy, qty, buyCcy):
        currencyPair = CurrencyPair(sellCcy, buyCcy)
        orders = self.DataProvider.AllPossibleTransfers[currencyPair]
        for order in orders:
            if order[0] == "Long":
                self.Book.buy(order[1].BaseCurrency, qty, order[1].MarketCurrency, self.DataProvider)
                qty = self.Book.get_balance()[order[1].MarketCurrency]
            else:
                self.Book.buy(order[1].MarketCurrency, qty, order[1].BaseCurrency, self.DataProvider)
                qty = self.Book.get_balance()[order[1].BaseCurrency]







