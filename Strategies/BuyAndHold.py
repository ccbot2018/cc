

class BuyAndHold:
    def __init__(self, currency):
        self.Book = None
        self.DataProvider = None
        self.State = "NoPosition"
        self.Currency = currency

    def Update(self, date):
        if self.State == "NoPosition":
            balance = self.Book.get_balance()
            for ccy, qty in balance.items():
                self.Book.buy(ccy, qty, self.Currency, self.DataProvider)
            self.State = "Holding"
            return self.Book.compute_mtm(self.DataProvider)
        elif self.State =="Holding":
            return self.Book.compute_mtm(self.DataProvider)




