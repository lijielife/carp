# -*- coding: utf-8 -*-
import os
import webbrowser
import pathlib
import numpy as np
import pandas as pd
from pyecharts import Kline, Overlap, Line, Grid, Bar, Page
import core.stock
import core.context
from core.api import TradeCalendar as cal


class StockBarRender(object):
    def __init__(self, bar):
        self.bar = bar
        self.success = False
        self.title = "%s-%s" % (bar.symbol, bar.freq)
        self.grid = Grid(width=1000, height=600)
        self.date = bar.date()

    def __setXaxis(self, n):
        self.xaxis = list(range(0, n))

    def __kline(self, **kwargs):
        overlap = Overlap()
        oclh = self.bar.oclh().values.tolist()
        kline = Kline(self.title)
        kline.add("", self.date, oclh, is_datazoom_show=True, tooltip_tragger='axis', tooltip_axispointer_type='cross',
                  datazoom_xaxis_index=self.xaxis, is_xaxis_show=False)
        # pyechars have no public options
        kline._option.get('yAxis')[0].update(scale=True, splitArea={"show": False})
        overlap.add(kline)
        if kwargs.get('MA') is not None:
            mas = kwargs.get('MA')
            overlap.add(self.__ma(*mas))
        return overlap

    def __ma(self, *args):
        if len(args) <= 0:
            return None
        _is_smooth = True
        ma_line = Line()
        ma_list = self.bar.MA(args)
        for i, ma in enumerate(ma_list):
            ma_line.add("MA%d" % args[i], self.date, ma, is_smooth=True)
        return ma_line

    def __volume(self):
        volumeBar = Bar()
        volumeBar.add("", self.date, self.bar.volume().values,
                      is_datazoom_show=True, is_xaxis_show=False)
        return volumeBar

    def __macd(self):
        dif, dea, macd = self.bar.macd()
        overlap = Overlap()
        macdBar = Bar()
        macdBar.add("", self.date, macd, is_datazoom_show=True)
        macdline = Line()
        macdline.add("", self.date, dif, is_smooth=True)
        macdline.add("", self.date, dea, is_smooth=True)
        overlap.add(macdBar)
        overlap.add(macdline)
        return overlap

    def get_view(self):
        self.__render_impl()
        return self.grid

    def __render_impl(self):
        if self.success == False:
            self.__setXaxis(3)
            self.grid.add(self.__kline(ma=[5, 10, 20]), grid_bottom='40%')
            self.grid.add(self.__volume(), grid_top='60%', grid_bottom='30%')
            self.grid.add(self.__macd(), grid_top='70%')
            self.success = True

    def render(self):
        self.__render_impl()
        filename = '%s.html' % self.title
        self.grid.render(filename)
        return filename


class WebPage(object):
    def __init__(self, name):
        self.name = name
        self.views = {}
        self.page = Page(name)

    def add(self, *args):
        for i in args:
            self.views[i.title] = i
            self.page.add(i.get_view())

    def show(self):
        filename = '%s.html' % (self.name)
        # self.page.render()
        self.page.render(filename)
        webbrowser.open_new_tab(pathlib.Path(os.path.abspath(filename)).as_uri())


if __name__ == "__main__":

    page = WebPage('test')

    stock = context.stock('300348.SZ')
    b1 = stock.bar(start=cal.day(_days=-300), end=cal.day())
    r1 = StockBarRender(b1)

    stock = context.stock('000001.SZ')
    b2 = stock.bar(start=cal.day(_days=-300), end=cal.day())
    r2 = StockBarRender(b2)
    page.add(r1, r2)
    page.show()
    # webbrowser.open_new_tab(pathlib.Path(os.path.abspath(r.render())).as_uri())
