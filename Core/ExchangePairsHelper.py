from Core.CurrencyPairs import CurrencyPair

def CreateAllPossiblePairs(currencyList):
    retVal = list()
    for currency in currencyList:
        for currency2 in currencyList:
            if currency is not currency2:
                retVal.append(CurrencyPair(currency, currency2))

    return retVal

def CreateAllPossibleTransfers(allCurrencyPairs, existingCurrencyPairs, refCcy):
    retVal = dict()
    existingCurrencyPairs = existingCurrencyPairs.keys()
    for currencyPair in allCurrencyPairs:
        if currencyPair in existingCurrencyPairs:
            retVal[currencyPair] = list([("Long", currencyPair)])
        elif currencyPair.GetReversePair() in existingCurrencyPairs:
            retVal[currencyPair] = list([("Short", currencyPair.GetReversePair())])
        else:
            ccy1 = currencyPair.BaseCurrency
            ccy2 = currencyPair.MarketCurrency
            ccy1Pairs = list(filter(lambda t: t.BaseCurrency == ccy1 or t.MarketCurrency == ccy1, existingCurrencyPairs))
            ccy2Pairs = list(filter(lambda t: t.BaseCurrency == ccy2 or t.MarketCurrency == ccy2, existingCurrencyPairs))
            temp = list()
            for ccy1Pair in ccy1Pairs:
                currentPath = list()
                if ccy1Pair.BaseCurrency == ccy1:
                    currentPath.append(["Long", ccy1Pair])
                    pivotCurrency = ccy1Pair.MarketCurrency
                else:
                    currentPath.append(["Short", ccy1Pair])
                    pivotCurrency = ccy1Pair.BaseCurrency
                if (currencyPair == currentPath[0][1] or currencyPair == currentPath[0][1].GetReversePair()):
                    stringList = [str(t[1]) if t[0] == "Long" else str(t[1].GetReversePair()) for t in
                                  retVal[currencyPair]]
                    print(str(currencyPair) + " " + " ".join(stringList))
                    break
                for ccy2Pair in ccy2Pairs:
                    if ccy2Pair.BaseCurrency == pivotCurrency:
                        temp.append(list(currentPath))
                        temp[-1].append(["Long", ccy2Pair])
                    elif ccy2Pair.MarketCurrency == pivotCurrency:
                        temp.append(list(currentPath))
                        temp[-1].append(["Short", ccy2Pair])
            if len(temp) == 0:
                print("Not possible for pair " + str(currencyPair))
            if len(temp) == 1:
                retVal[currencyPair] = temp[0]
            elif len(temp) > 1:
                filteredList = list(filter(lambda t: t[0][1].BaseCurrency == refCcy or t[0][1].MarketCurrency == refCcy, temp))
                if len(filteredList)>0:
                    retVal[currencyPair] = filteredList[0]
                else:
                    retVal[currencyPair] = temp[0]
        stringList = [str(t[1]) if t[0] == "Long" else str(t[1].GetReversePair()) for t in  retVal[currencyPair]]
        print (str(currencyPair) + " " + " ".join(stringList))
    return retVal


