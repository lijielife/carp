#!/usr/bin/env python
# -*- coding: utf-8 -*-



import datetime
import arrow
from carp import composite
from carp import trade_calendar
from carp import util
import numpy as np
import pandas as pd
from carp import kline




def test_stock_basic():
    basic = composite.get_basic()
    basic.clear()

    date1 = basic.date()
    assert date1 is None

    df1 = pd.read_csv('./debug/get_stock_basic_action.csv')
    date2 = trade_calendar.last_trade_date(util.FREQ_DAY)
    df1 = df1[df1['timeToMarket'] != 0]
    df1['timeToMarket'] = pd.to_datetime(df1['timeToMarket'].astype(str), format='%Y%m%d').astype(str)
    basic.save(df1)
    print(date2)
    print(basic.date())
    assert date2 == basic.date()
    assert basic.count() == 3499
    assert basic.info('000002')['name'] == '万 科Ａ'
    assert basic.info('000002')['timeToMarket'] == '1991-01-29'


def test_load():
    helper = kline.KlinedbHelper.load_db_helper('DAY')
    start = trade_calendar.create_v2('2018-01-10',  util.FREQ_DAY)
    end = trade_calendar.create_v2('2018-03-10', util.FREQ_DAY)
    k = helper.load('000002',start, end)
    assert k.empty is False



def test_datetime():
    v1 = trade_calendar.create_v2('2018-01-01', freq = util.FREQ_DAY)
    assert v1.datetime().year == 2017 and v1.datetime().month == 12 and v1.datetime().day == 29


def test_tradecalendar_create():
    v1 = trade_calendar.create_v2( "2018-01-01", freq = util.FREQ_DAY)
    assert v1.datetime().year == 2017 and v1.datetime().month == 12 and v1.datetime().day == 29


def test_shift():
    v1 = trade_calendar.create_v2("2018-02-10", freq = util.FREQ_DAY)
    v1.shift(1)
    assert v1.datetime().year == 2018 and v1.datetime().month == 2 and v1.datetime().day == 12

    v1.shift(3)
    assert v1.datetime().year == 2018 and v1.datetime().month == 2 and v1.datetime().day == 22

def test_trade_calendarv2():
    v1 = trade_calendar.create_v2('2018-01-01', util.FREQ_DAY)
    assert (v1.shift(1) == trade_calendar.create_v2('2018-01-02', util.FREQ_DAY))
    assert (v1.shift(-4) == trade_calendar.create_v2('2017-12-26', util.FREQ_DAY))

    v2 = trade_calendar.create_v2('2018-01-01 10:30:00', util.FREQ_30M)
    assert (v2.shift(1) == trade_calendar.create_v2('2018-01-02 09:30:00', util.FREQ_30M))
    assert (v2.shift(-4) == trade_calendar.create_v2('2017-12-29 13:30:00', util.FREQ_30M))

    assert trade_calendar.compare_v2('2018-01-01', '2017-12-31', util.FREQ_DAY) == 0


