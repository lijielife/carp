#!/usr/bin/env python
# -*- coding: utf-8 -*-

from carp import context
from carp import util
from carp import render



## sync 数据
context.sync_history_bar()


## 获取stocks list
symbols = context.get_stock_list()
print(symbols)


basic = context.get_basic()

print(context.get_date_bars('000002', '2015-11-11', '2018-05-11', freq = util.FREQ_DAY))
print(context.get_count_bars('000002', None, limit = 20, freq = util.FREQ_DAY))


r = render.StockBarRender.create('000002', '2017-10-10', '2018-03-20', util.FREQ_DAY)

page = render.WebPage('test')
page.add(r)
page.show()
