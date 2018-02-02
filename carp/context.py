# -*- coding: utf-8 -*-

import time
from carp import task
from carp import composite
from carp import util
from carp import config
from carp import kline
from carp import trade_calendar
import pandas as pd



def sync_history_bar():
    basic = task.request_stock_basic()
    if basic is not None:
        df = basic.df()
        # TODO add other freq
        for freq in config.KlineConfig.SYNC_FREQS:
            task.sync_history_bar(df.index, freq)


def get_count_bars(code, last, limit, freq,  ascending = False):
    query = kline.KlineQuery(code)
    if last is None:
        end = trade_calendar.last_trade_date(freq)
    else:
        end = trade_calendar.create_v2(last, freq)
    return query.load(None, end, freq, ascending = ascending, limit = limit)


def get_date_bars(code, start, end, freq, ascending = False):
    query = kline.KlineQuery(code)
    return query.load(start, end, freq, ascending = ascending)


def get_basic():
    return composite.get_basic()



def get_stock_list():
    return composite.get_basic().df().index.values


