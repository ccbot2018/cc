import itertools as it
from Base.CurrencyPairs import CurrencyPair, BuySell

class TriangularArbitrageur:
    def __init__(self, exchangeData, fees):
        self.ExchangeData = exchangeData
        self.Fees = fees
        self.Currencies, self.BaseCurrencies = self.__extractCurrencyList()
        self.AllArbitrageablePairs =  self.__findAllArbitrageablePairs()
        self.Triangles = self.__buildAllTriangles()

    def __extractCurrencyList(self):
        currencies = set()
        baseCurrencies = set()
        for pairName, pair in self.ExchangeData.items():
            if not pairName.BaseCurrency in currencies:
                currencies.add(pairName.BaseCurrency)
            if not pairName.BaseCurrency in baseCurrencies:
                baseCurrencies.add(pairName.BaseCurrency)
            if not pairName.MarketCurrency in currencies:
                currencies.add(pairName.MarketCurrency)
        return currencies, baseCurrencies

    def __findAllArbitrageablePairs(self):
        retVal = set()
        for currency in self.Currencies:
            temp = list()
            baseCurrencies = set()
            for baseCcy in self.BaseCurrencies:
                currencyPair = CurrencyPair(baseCcy, currency)
                if currencyPair in self.ExchangeData:
                    temp.append(currencyPair)
                    baseCurrencies.add(baseCcy)
            if len(temp) == 2:
                temp = self.__addBasePairs(baseCurrencies, temp)
                retVal.add(TriangularPair(temp[0], temp[1], temp[2]))
            elif len(temp) == 3:
                temp = self.__addBasePairs(baseCurrencies, temp)
                triangularPairs = self.__createTriangularPairs(temp)
                retVal.update(triangularPairs)
        return retVal

    def __createTriangularPairs(self, pairsList):
        combinations = list(it.combinations(pairsList, 3))
        retVal = list()
        for comb in combinations:
            currencies = set()
            currencies.add(comb[0].BaseCurrency)
            currencies.add(comb[0].MarketCurrency)
            currencies.add(comb[1].BaseCurrency)
            currencies.add(comb[1].MarketCurrency)
            currencies.add(comb[2].BaseCurrency)
            currencies.add(comb[2].MarketCurrency)
            if len(currencies) == 3:
                retVal.append(TriangularPair(comb[0], comb[1], comb[2]))
        return retVal

    def __addBasePairs(self, baseCurrencies, pairsList):
        basePairs = list(map(lambda t: CurrencyPair(t[0], t[1]), it.combinations(baseCurrencies, 2)))
        for pair in basePairs:
            revPair = pair.GetReversePair()
            if pair in self.ExchangeData:
                pairsList.append(pair)
            elif revPair in self.ExchangeData:
                pairsList.append(revPair)
        return pairsList

    def __buildAllTriangles(self):
        retVal = list()
        for triangularPair in self.AllArbitrageablePairs:
            retVal.extend(triangularPair.BuildTriangles(self.ExchangeData))
        return retVal

    def EvaluateArbitragePossibilities(self, marketData):
        for triangle in self.Triangles:
            nUnits2 = self.__passOrder(triangle.Pair1, triangle.BuySell1, 1.0, marketData)
            nUnits3 = self.__passOrder(triangle.Pair2, triangle.BuySell2, nUnits2, marketData)
            nUnits1 = self.__passOrder(triangle.Pair3, triangle.BuySell3, nUnits3, marketData)
            if nUnits1 > 1.0:
                print ("There is an arbitrage for triangle " + str(triangle) + " " + "{0:.2f}%".format(100*(nUnits1 - 1.0)))
            #else:
                #print("There is no arbitrage for triangle " + str(triangle) + " " + "{0:.2f}%".format(100*(nUnits1 - 1.0)))

    def __passOrder(self, currencyPair, buySell, baseUnits, marketData):
        if buySell == BuySell.Buy:
            ask = marketData[currencyPair].Ask
            cc2Position = baseUnits / ask *(1-self.Fees)
        else:
            bid = marketData[currencyPair].Bid
            cc2Position = bid * baseUnits*(1-self.Fees)
        return cc2Position

class TriangularPair:
    def __init__(self, currencyPair1, currencyPair2, currencyPair3):
        self.CurrencyPairs = set()
        self.CurrencyPairs.add(currencyPair1)
        self.CurrencyPairs.add(currencyPair2)
        self.CurrencyPairs.add(currencyPair3)
        self.MarketCurrencies = set([t.MarketCurrency for t in self.CurrencyPairs])
        self.BaseCurrencies = set([t.BaseCurrency for t in self.CurrencyPairs])
        self.ReversePairs = set([t.GetReversePair() for t in self.CurrencyPairs])
        self.Currencies = self.MarketCurrencies | self.BaseCurrencies

    def __key(self):
        return tuple(self.CurrencyPairs)

    def __eq__(x, y):
        return x.__key() == y.__key()

    def __hash__(self):
        return hash(self.__key())

    def __str__(self):
        return ",".join(self.Currencies)

    def BuildTriangles(self, marketData):
        retVal = list()
        allPermutations = list(it.permutations(self.Currencies))
        for permutation in allPermutations:
            if permutation[0] == 'BTC':
                firstPair, buySell1 = self.__buySellCcyPair(permutation[0], permutation[1], marketData)
                secondPair, buySell2 = self.__buySellCcyPair(permutation[1], permutation[2], marketData)
                thirdPair, buySell3 = self.__buySellCcyPair(permutation[2], permutation[0], marketData)
                retVal.append(Triangle(firstPair, buySell1, secondPair, buySell2, thirdPair, buySell3))
        return retVal

    def __buySellCcyPair(self, ccy1, ccy2, marketData):
        pair = CurrencyPair(ccy1, ccy2)
        if pair in marketData:
            buySell = BuySell.Buy
        else:
            pair = pair.GetReversePair()
            buySell = BuySell.Sell
        return pair, buySell


class Triangle:
    def __init__(self, pair1, buySell1, pair2, buySell2, pair3, buySell3):
        self.Pair1 = pair1
        self.Pair2 = pair2
        self.Pair3 = pair3
        self.BuySell1 = buySell1
        self.BuySell2 = buySell2
        self.BuySell3 = buySell3

    def __str__(self):
        a = str(self.Pair1) if self.BuySell1 == BuySell.Buy else str(self.Pair1.GetReversePair())
        b = str(self.Pair2) if self.BuySell2 == BuySell.Buy else str(self.Pair2.GetReversePair())
        c = str(self.Pair3) if self.BuySell3 == BuySell.Buy else str(self.Pair3.GetReversePair())
        return "=>".join([a,b,c])






