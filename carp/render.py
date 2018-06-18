# -*- coding: utf-8 -*-
import os
import webbrowser
import talib
import pathlib
import numpy as np
import pandas as pd
from pyecharts import Kline, Overlap, Line, Grid, Bar, Page
from carp import context
from carp import util


class StockBarRender(object):

    @staticmethod
    def create(symbol, start, end, freq):
        df = context.get_date_bars(symbol, start, end, freq, True)
        return StockBarRender(symbol, df, freq)

    def __init__(self, symbol, df, freq):
        self.success = False
        self.__symbol = symbol
        self.__freq = freq
        self.title = '%s-%s-%s' % (symbol, freq, context.get_basic().info(symbol)['name'])
        self.__df = df.drop(df[df['high'] <= 0].index)
        self.__date = self.__df['datetime']
        self.grid = Grid(width=1000, height=600)


    def __setXaxis(self, n):
        self.xaxis = list(range(0, n))

    def __kline(self, **kwargs):
        overlap = Overlap()
        oclh = self.__df[['open', 'close', 'low', 'high']].values.tolist()
        kline = Kline(self.title)
        kline.add("", self.__date, oclh, is_datazoom_show=True, tooltip_tragger='axis', tooltip_axispointer_type='cross',
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
        ma_list = []
        close = self.__df['close'].values
        print(*args)
        for i, val in enumerate(args):
            if 'MA' + str(val) in self.__df.keys():
                ma_list.append(self.__df['MA' + str(val)])
            else:
                real = talib.MA(close, timeperiod=val)
                self.__df['MA' + str(val)] = real
                ma_list.append(real)

        for i, ma in enumerate(ma_list):
            ma_line.add("MA%d" % args[i], self.__date, ma, is_smooth=False)
        return ma_line


    def __volume(self):
        volumeBar = Bar()
        volumeBar.add("", self.__date, self.__df['vol'].values,
                      is_datazoom_show=True, is_xaxis_show=False)
        return volumeBar

    def __macd(self):
        ret = []
        keys = self.__df.keys()
        df = self.__df
        if 'dif' in keys and 'dea' in keys and 'macd' in keys:
            return [df['dif'], df['dea'], df['macd']]
        else:
            close = df['close'].values
            df['dif'], df['dea'], df['macd'] = talib.MACD(
                close, fastperiod=12, slowperiod=26, signalperiod=9)
            return df['dif'], df['dea'], df['macd']

    def MACD(self):
        dif, dea, macd = self.__macd()
        overlap = Overlap()
        macdBar = Bar()
        macdBar.add("", self.__date, macd, is_datazoom_show=True,
            is_visualmap=True, is_piecewise=True, pieces = [
                {'min': 0, 'max':300,  'color': "#f47920"},
                {'min': -300, 'max': 0,  'color': "#f6f5ec"},
                ])
        macdline = Line()
        macdline.add("", self.__date, dif, is_smooth=False)
        macdline.add("", self.__date, dea, is_smooth=False)
        overlap.add(macdBar)
        overlap.add(macdline)
        return overlap

    def get_view(self):
        self.__render_impl()
        return self.grid

    def __render_impl(self):
        if self.success == False:
            self.__setXaxis(3)
            self.grid.add(self.__kline(MA=[5, 10, 20, 30]), grid_bottom='50%')
            self.grid.add(self.__volume(), grid_top='50%', grid_bottom='40%')
            self.grid.add(self.MACD(), grid_top='60%')
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
        self.page.render(filename)
        webbrowser.open_new_tab(pathlib.Path(os.path.abspath(filename)).as_uri())


