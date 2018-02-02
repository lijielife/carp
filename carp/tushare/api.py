# -*- coding: utf-8 -*-

import tushare as ts
import pandas as pd


def tushare_get_stock_basic():
    df = ts.get_stock_basics()
    if isinstance(df, pd.DataFrame) and df.empty == False:
        return df
    else:
        raise TypeError('df is unknown type %s' % type(df))



def tushare_bar(code, start, end, freq, **kwargs):
    df = ts.bar(code=code, conn = ts.get_apis(), start_date = start, end_date = end, \
            freq = freq, asset='E', adj = 'qfq', **kwargs)
    if isinstance(df, pd.DataFrame):
        return df
    else:
        raise TypeError('df is unknown type %s' % type(df))


def tushare_get_profit(year, month):
    #TODO review
    return ts.get_profit_data(year, month)


def tushare_get_today_bar():
    df = ts.get_today_all()
    if isinstance(df, pd.DataFrame):
        return df
    else:
        raise TypeError('df is unknown type %s' % type(df))








