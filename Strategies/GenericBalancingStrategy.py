import datetime as dt
from Core.Frequency import FREQUENCIES_DICT

class GenericBalancingStrategy:
    def __init__(self, rebalancingFrequency, stopLossFrequency, currencyPicker, stopLossLevel):
        self.Book = None
        self.DataProvider = None
        self.State = "NotStarted"
        self.RebalancingFrequency = FREQUENCIES_DICT[rebalancingFrequency]
        self.StopLossFrequency = FREQUENCIES_DICT[stopLossFrequency]
        self.LastReBalancing = dt.datetime(0,0,0)
        self.LastStopLossMonitoring = dt.datetime(0,0,0)
        self.CurrencyPicker = currencyPicker
        self.StopLossLevel = stopLossLevel
        self.LastReBalancingMtM = dict()
        self.RefCcy = self.DataProvider.RefCurrency

    def Update(self, date):
        if self.State == "NotStarted":
            currenciesToHold = self.CurrencyPicker.Pick()
            self.RebalanceBook(currenciesToHold)
            self.State = "LongPositions"
            self.LastReBalancing = date
            self.LastReBalancingMtM = self.Book.ComputeMtMPerHolding()
        elif self.State == "LongPositions" and date >= (self.LastReBalancing + self.RebalancingFrequency):
            currenciesToHold = self.CurrencyPicker.Pick()
            self.RebalanceBook(currenciesToHold)
            self.State = "LongPositions"
            self.LastReBalancing = date
            self.LastReBalancingMtM = self.Book.ComputeMtMPerHolding()
        elif self.State == "LongPositions" \
            and (self.LastReBalancing + self.RebalancingFrequency) > date > (self.LastStopLossMonitoring + self.StopLossFrequency):
            self.LastStopLossMonitoring = date


    def MonitorStopLosses(self):
        currentHoldings =  self.Book.ComputeMtMPerHolding(self.DataProvider)
        for currency in [t for t in currentHoldings.keys() if t is not self.RefCcy]:
            if currentHoldings[currency] <= (1-self.StopLossLevel)*self.LastReBalancingMtM[currency]:
                self.LiquidateCurrency(currency)


    def LiquidateCurrency(self, sellCcy):
        orders = self.DataProvider.ToRefCcy[sellCcy]
        qty = self.Book.GetBalance()[sellCcy]
        for order in orders:
            if (order is "Identity"):
                continue
            self.Book.Buy(sellCcy, qty, order[1].MarketCurrency, self.DataProvider)
            qty = self.Book.GetBalance()[order[1].MarketCurrency]
            sellCcy = order[1].MarketCurrency

    def RebalanceBook(self, currencyList):





