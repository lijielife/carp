# -*- coding: utf-8 -*-
import os
import numpy as np
from core.api import singleton, Api, TradeCalendar
from core.composite import Composite
from core.stock import StockBarLoader, StockFactory


_API = Api()
_COMPOSITE = Composite()
_STOCK_FACTORY = StockFactory()


def sync(**kwargs):
    StockBarLoader.sync_stock_bar(**kwargs)


def stock(symbol):
    if symbol in _COMPOSITE.get_all_symbols():
        return _STOCK_FACTORY.stock(symbol)
    else:
        raise Exception("invalid symbol %s" % (symbol))


def lefted_symbols(day=-3):
    start, end = TradeCalendar.duration(_days=day)
    df = _API.secrestricted(_start_date=start, _end_date=end)
    return [] if df.empty() else np.unique(df.index().values)


def get_all_symbols():
    return _COMPOSITE.get_all_symbols()


if __name__ == "__main__":
    args = {
        'cache': {
            '1d': 1440,
            '1w': 48 * 4,
        }
    }
    sync(**args)
