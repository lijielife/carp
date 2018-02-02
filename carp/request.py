# -*- coding: utf-8 -*-

import os
import time
import pandas as pd
import carp.tushare as ts
from carp import util
from carp.util import log
from carp.config import GlobalConfig as CONFIG
import numpy as np


def __load_from_csv(filename):
    filename = os.path.join(CONFIG.DEBUG_PATH, filename)
    if not os.path.exists(filename):
        return None
    try:
        df = pd.read_csv(filename)
        pd.DataFrame(df)
        return df
    except ValueError:
        log.error('load dataframe error')
        return None

def __save_to_csv(df, filename):
    filename = os.path.join(CONFIG.DEBUG_PATH, filename)
    df.to_csv(filename)


def __debug_df_to_csv(debug):
    def inner_func(func):
        def wrapper(*args, **kwargs):
            if debug:
                filename = func.__name__ + '.csv'
                df = __load_from_csv(filename)
                if df is None:
                    df = func(*args, **kwargs)
                    __save_to_csv(df, filename)
            else:
                df = func(*args, **kwargs)
            return df
        return wrapper
    return inner_func


@__debug_df_to_csv(debug=CONFIG.DEBUG)
def request_stock_bar(code, start, end, freq):
    times = 0
    while times < 100:
        try:
            df = __get_stock_bar(code, start, end, freq)
        except OSError:
            sec = 30 + times * 10
            log.info('retry %d times sleep %ds' % (times, sec))
            time.sleep(sec)
            times += 1
            continue
        break

    if not isinstance(df, pd.DataFrame):
        raise RuntimeError('request %s history error' % code)
    return df

def __get_stock_bar(code, start, end, freq):
    df = None
    _start = start.format()
    _end = end.format()
    if freq == util.FREQ_DAY or freq == util.FREQ_WEEK or freq == util.FREQ_MONTH:
        #covert = {util.FREQ_DAY:'1d', util.FREQ_WEEK:'1w', util.FREQ_MONTH: '1m'}
        #df = datacore_daily(symbol = code, start_date = start, end_date = end, freq = covert[freq])
        covert = {util.FREQ_DAY:'D', util.FREQ_WEEK:'W', util.FREQ_MONTH: 'M'}
        df = ts.get_stock_bar(code = code, start = _start, end = _end, freq = covert[freq], factors=['vr', 'tor'])
    else:
        # '1M' '5M' '15M' '30M'
        covert = {util.FREQ_1M:'1MIN', util.FREQ_5M:'5MIN', util.FREQ_15M : '15MIN', util.FREQ_30M :'30MIN'}
        df = ts.tushare_bar(code = code, start = _start, end = _end, freq = covert[freq], factors=['vr', 'tor'])
        ## date to int

    if not isinstance(df, pd.DataFrame):
        raise TypeError('df is unknown type %s' % type(df))
    elif df.empty:
        log.warn('%s from %s to %s bar[%s] is empty' % (code, start, end, freq))
    return df


def get_today_all():
    return ts.get_today_all()


@__debug_df_to_csv(debug=CONFIG.DEBUG)
def request_stock_basic():
    return ts.get_stock_basic()


@__debug_df_to_csv(debug=CONFIG.DEBUG)
def request_stock_profit(year, month):
    return ts.tushare_get_profit(year, month)

