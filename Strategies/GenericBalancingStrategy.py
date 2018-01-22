import datetime as dt
from Core.CurrencyPairs import CurrencyPair

class GenericBalancingStrategy:
    def __init__(self, rebalancingFrequency, stopLossFrequency, currencyPicker, stopLossLevel, pivotCurrency):
        self.Book = None
        self.DataProvider = None
        self.State = "NotStarted"
        self.RebalancingFrequency = rebalancingFrequency
        self.StopLossFrequency = stopLossFrequency
        self.LastReBalancing = dt.datetime(2015,1,1)
        self.LastStopLossMonitoring = dt.datetime(2015,1,1)
        self.CurrencyPicker = currencyPicker
        self.StopLossLevel = stopLossLevel
        self.LastReBalancingMtM = dict()
        self.PivotCcy = pivotCurrency

    def Update(self, date):
        if self.State == "NotStarted":
            currenciesToHold = self.CurrencyPicker.Pick(self.DataProvider)
            self.LastReBalancingMtM = self.Book.ComputeMtMPerHolding(self.DataProvider)
            self.__rebalanceBook(currenciesToHold)
            self.State = "LongPositions"
            self.LastReBalancing = date
            self.LastReBalancingMtM = self.Book.ComputeMtMPerHolding(self.DataProvider)
            return self.Book.ComputeMtM(self.DataProvider)
        elif self.State == "LongPositions" and date >= (self.LastReBalancing + self.RebalancingFrequency):
            currenciesToHold = self.CurrencyPicker.Pick(self.DataProvider)
            self.__rebalanceBook(currenciesToHold)
            self.State = "LongPositions"
            self.LastReBalancing = date
            self.LastReBalancingMtM = self.Book.ComputeMtMPerHolding(self.DataProvider)
            return self.Book.ComputeMtM(self.DataProvider)
        elif self.State == "LongPositions" \
            and (self.LastReBalancing + self.RebalancingFrequency) > date > (self.LastStopLossMonitoring + self.StopLossFrequency):
            self.LastStopLossMonitoring = date
            self.__monitorStopLosses()
            self.LastReBalancingMtM = self.Book.ComputeMtMPerHolding(self.DataProvider)
            return self.Book.ComputeMtM(self.DataProvider)


    def __monitorStopLosses(self):
        currentHoldings =  self.Book.ComputeMtMPerHolding(self.DataProvider)
        for currency in [t for t in currentHoldings.keys() if t is not self.DataProvider.RefCurrency and currentHoldings[t] >0]:
            if currentHoldings[currency] <= (1-self.StopLossLevel)*self.LastReBalancingMtM[currency]:
                print("Stop loss activated for " + currency)
                self.__liquidateCurrency(currency)



    def __liquidateCurrency(self, sellCcy):
        currencyPair = CurrencyPair(sellCcy, self.DataProvider.RefCurrency)
        orders = self.DataProvider.AllPossibleTransfers[currencyPair]
        qty = self.Book.GetBalance()[sellCcy]
        for order in orders:
            qty = self.__executeOrder(order, qty)

    def __rebalanceBook(self, currencyList):
        currentHoldings = set([t for t in self.LastReBalancingMtM.keys() if self.LastReBalancingMtM[t] > 0])
        newCurrenciesToHold = set(currencyList) - currentHoldings
        keepHoldingCurrencies = currentHoldings & newCurrenciesToHold
        liquidateCurrencies = currentHoldings - keepHoldingCurrencies
        for ccy in [t for t in liquidateCurrencies if t != self.PivotCcy]:
            currencyPair = CurrencyPair(ccy, self.PivotCcy)
            orders = self.DataProvider.AllPossibleTransfers[currencyPair]
            qty = self.Book.GetBalance()[ccy]
            for order in orders:
                qty = self.__executeOrder(order, qty)
        if len(newCurrenciesToHold)>0:
            qtyToBuy = self.Book.GetBalance()[self.PivotCcy]/len(newCurrenciesToHold)
            for ccy in [t for t in newCurrenciesToHold if t != self.PivotCcy]:
                currencyPair = CurrencyPair(self.PivotCcy, ccy)
                orders = self.DataProvider.AllPossibleTransfers[currencyPair]
                qty = qtyToBuy
                for order in orders:
                    qty = self.__executeOrder(order, qty)


    def __executeOrder(self, order, qty):
        if order[0] == "Long":
            self.Book.Buy(order[1].BaseCurrency, qty, order[1].MarketCurrency, self.DataProvider)
            return self.Book.GetBalance()[order[1].MarketCurrency]
        else:
            self.Book.Buy(order[1].MarketCurrency, qty, order[1].BaseCurrency, self.DataProvider)
            return self.Book.GetBalance()[order[1].BaseCurrency]





