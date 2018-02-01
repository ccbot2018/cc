from Core.CurrencyPairs import CurrencyPair


def create_all_possible_pairs(currency_list):
    ret_val = list()
    for currency in currency_list:
        for currency2 in currency_list:
            if currency is not currency2:
                ret_val.append(CurrencyPair(currency, currency2))
    return ret_val


def create_all_possible_transfers(all_currency_pairs, existing_currency_pairs, ref_ccy):
    ret_val = dict()
    existing_currency_pairs = existing_currency_pairs.keys()
    for currencyPair in all_currency_pairs:
        if currencyPair in existing_currency_pairs:
            ret_val[currencyPair] = list([("Long", currencyPair)])
        elif currencyPair.get_reverse_pair() in existing_currency_pairs:
            ret_val[currencyPair] = list([("Short", currencyPair.get_reverse_pair())])
        else:
            ccy1 = currencyPair.BaseCurrency
            ccy2 = currencyPair.MarketCurrency
            ccy1_pairs = list(
                filter(lambda t: t.BaseCurrency == ccy1 or t.MarketCurrency == ccy1, existing_currency_pairs))
            ccy2_pairs = list(
                filter(lambda t: t.BaseCurrency == ccy2 or t.MarketCurrency == ccy2, existing_currency_pairs))
            temp = list()
            for ccy1Pair in ccy1_pairs:
                current_path = list()
                if ccy1Pair.BaseCurrency == ccy1:
                    current_path.append(["Long", ccy1Pair])
                    pivot_currency = ccy1Pair.MarketCurrency
                else:
                    current_path.append(["Short", ccy1Pair])
                    pivot_currency = ccy1Pair.BaseCurrency
                if currencyPair == current_path[0][1] or currencyPair == current_path[0][1].get_reverse_pair():
                    string_list = [str(t[1]) if t[0] == "Long" else str(t[1].get_reverse_pair()) for t in
                                   ret_val[currencyPair]]
                    print(str(currencyPair) + " " + " ".join(string_list))
                    break
                for ccy2Pair in ccy2_pairs:
                    if ccy2Pair.BaseCurrency == pivot_currency:
                        temp.append(list(current_path))
                        temp[-1].append(["Long", ccy2Pair])
                    elif ccy2Pair.MarketCurrency == pivot_currency:
                        temp.append(list(current_path))
                        temp[-1].append(["Short", ccy2Pair])
            if len(temp) == 0:
                print("Not possible for pair " + str(currencyPair))
            if len(temp) == 1:
                ret_val[currencyPair] = temp[0]
            elif len(temp) > 1:
                filtered_list = list(
                    filter(lambda t: t[0][1].BaseCurrency == ref_ccy or t[0][1].MarketCurrency == ref_ccy, temp))
                if len(filtered_list) > 0:
                    ret_val[currencyPair] = filtered_list[0]
                else:
                    ret_val[currencyPair] = temp[0]
        string_list = [str(t[1]) if t[0] == "Long" else str(t[1].get_reverse_pair()) for t in ret_val[currencyPair]]
        print(str(currencyPair) + " " + " ".join(string_list))
    return ret_val
