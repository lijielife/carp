# -*- coding: utf-8 -*-
import sys
sys.path.append("../src/")
import api
from api import TradeCalendar
import datetime
import logging

__api = api.Api()
logging.basicConfig(level=logging.DEBUG)
__log = logging.getLogger("DEBUG")

def test_api():
    global api
    instrumentdf = __api.instrumentinfo()
    #__log.debug(instrumentdf(api.InstrumentinfoDataFrame.NAME))
    #__log.debug(instrumentdf.query(api.InstrumentinfoDataFrame.NAME, '平安银行'))
    assert instrumentdf.empty() is False
    assert __api.secsusp().empty() is False
    assert __api.secrestricted(_start_date = TradeCalendar.day(), _end_date = TradeCalendar.day(_days = 30)).empty() is False
    assert __api.tradecal().empty() is False
    filterstr = __api.format_filter(test1='value1', test2= "value2")
    assert (filterstr == 'test1=value1&test2=value2' or filterstr == 'test2=value2&test1=value1')
    assert __api.daily(_symbol='000002.SZ', _start_date=TradeCalendar.day(_days=-10),
            _end_date=TradeCalendar.day()).empty() is False
    assert __api.bar(_symbol='000002.SZ', _trade_date = 20171212).empty() is False
