import numpy as np

class BestPerformersStrategy:
    def __init__(self, numberOfCurrenciesToPick, observationWindow):
        self.NumberOfCurrenciesToPick = numberOfCurrenciesToPick
        self.ObservationWindow = observationWindow


    def Pick(self, dataProvider):
        timeSeries = dataProvider.GetAllTimeSerieClose()
        ccyList = list(dataProvider.CurrencyList)
        ccyTimeSeries = timeSeries[ccyList]
        returns = (ccyTimeSeries - ccyTimeSeries.shift(self.ObservationWindow)) / ccyTimeSeries.shift(self.ObservationWindow)
        mostRecentReturn = returns.tail(1)
        order = np.argsort(-mostRecentReturn.values, axis=1)[:, :self.NumberOfCurrenciesToPick]
        chosenCcy = list(mostRecentReturn.columns[order][0])
        chosenCcy2 = [t for t in chosenCcy if mostRecentReturn[t][0] >= 0.02]
        return chosenCcy2



