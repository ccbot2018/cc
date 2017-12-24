

class BuyAndHold:
    def __init__(self, currency):
        self.Book = None
        self.DataProvider = None
        self.State = "NoPosition"
        self.Currency = currency

    def Update(self):
        if self.State == "NoPosition":
            balance = self.Book.GetBalance()
            for ccy, qty in balance.items():
                self.Book.Buy(ccy, qty, self.Currency, self.DataProvider)
            self.State = "Holding"
            return self.Book.ComputeMtM(self.DataProvider)
        elif self.State =="Holding":
            return self.Book.ComputeMtM(self.DataProvider)




