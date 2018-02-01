
class BacktestingExchangeBook:
    def __init__(self, initial_balance, fees, bid_ask_spread):
        self.balance = initial_balance
        self.orders = list()
        self.bid_ask_spread = bid_ask_spread
        self.fees = fees

    def buy(self, sell_currency, quantity_sell_currency, buy_currency, data_provider):
        if sell_currency in self.balance and self.balance[sell_currency] >= quantity_sell_currency:
            self.balance[sell_currency] -= quantity_sell_currency
            buy_ccy_price = data_provider.get_current_close(buy_currency)
            sell_ccy_price = data_provider.get_current_close(sell_currency)
            qty_buy_currency = (quantity_sell_currency * sell_ccy_price /
                                (buy_ccy_price * (1.0 + self.bid_ask_spread / 2.0))) \
                               * (1.0 - self.fees)
            if buy_currency in self.balance:
                self.balance[buy_currency] += qty_buy_currency
            else:
                self.balance[buy_currency] = qty_buy_currency
            self.orders.append(
                (data_provider.get_time(), sell_currency, quantity_sell_currency, buy_currency, qty_buy_currency))
            print((sell_currency, quantity_sell_currency, buy_currency, qty_buy_currency))
        else:
            return False

    def get_balance(self):
        return {k: v for k, v in self.balance.items() if v > 0}

    def compute_mtm_per_holding(self, data_provider):
        ret_val = dict()
        for currency, quantity in self.balance.items():
            currency_price = data_provider.get_current_close(currency)
            ret_val[currency] = quantity * currency_price
        return ret_val

    def compute_mtm(self, data_provider):
        mtm = 0
        for currency, quantity in self.balance.items():
            currency_price = data_provider.get_current_close(currency)
            mtm += quantity * currency_price
        return mtm
