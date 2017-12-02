# -*- coding: utf-8 -*-
import os
import datetime
import talib
import numpy as np
import pandas as pd
from core.api import TradeCalendar as cal, singleton, Api, DailyDataFrame
from core.util import logger, LOCAL_CACHE_PATH, JsonLoader, StockStore
from core.composite import Composite

FREQ_1M = '1M'
FREQ_5M = '5M'
FREQ_15M = '15M'
FREQ_DAY = '1d'
FREQ_WEEK = '1w'
FREQ_MONTH = '1m'


class StockBarLoader(object):
    __api = Api()

    __CACHE_CONFIG = os.path.join(LOCAL_CACHE_PATH, 'k', 'cache.json')
    if not os.path.exists(__CACHE_CONFIG):
        JsonLoader.create(__CACHE_CONFIG)

    @classmethod
    def get_sync_date(cls):
        data = {}
        config = cls.__CACHE_CONFIG
        l = JsonLoader.create(config)
        date = l.get('date')
        return None if date is None else cal.string2date(date)

    @classmethod
    def update_sync_date(cls):
        data = {}
        config = cls.__CACHE_CONFIG
        l = JsonLoader.create(config)
        l.put(**{'date': cal.now().strftime('%Y-%m-%d')})
        l.sync()

    @staticmethod
    def sync_stock_bar(**args):
        now = cal.now()
        symbols = Composite().get_all_symbols()
        last = StockBarLoader.get_sync_date()
        if last is None or last < now:
            c = StockBarLoader()
            for symbol in symbols:
                for k in args:
                    if k == 'cache':
                        for freq in args[k]:
                            logger.info("begin to sync %s[%s]" % (symbol, freq))
                            c.set(symbol)
                            c.refresh(duration=args[k][freq], freq=freq)
                    else:
                        pass
                StockBarLoader.update_sync_date()
        else:
            logger.info("stock has update last date")

        pass

    @classmethod
    def get_store(cls, symbol):
        return StockStore(symbol)

    @classmethod
    def get_freq_key(self, freq):
        return '/freq/' + 'F' + freq

    def __init__(self):
        self.symbol = ""
        self.freq = "1d"
        self.store = None

    def __del__(self):
        if self.store is not None:
            self.store.close()
            self.store = None

    def release(self):
        if self.store is not None:
            logger.debug('%s store close' % (self.symbol))
            self.store.close()
            self.store = None

    def set(self, symbol):
        if self.symbol != symbol:
            self.symbol = symbol
            if self.store is not None and self.store.is_open():
                self.store.close()
            self.store = StockBarLoader.get_store(symbol)

    def get_store_last_date(self):
        key = StockBarLoader.get_freq_key(self.freq)
        if key in self.store.keys():
            row = len(self.store.get(key).index)
            df = self.store.select(key, start=row - 1, end=row)
            if df.empty:
                logger.debug('store is empty, request all')
                return None
            else:
                if df.iloc[0][DailyDataFrame.TRADE_STATUS] == DailyDataFrame.TRADE_SUSPENSION:
                    logger.debug('%s [%s]' % (self.symbol, DailyDataFrame.TRADE_SUSPENSION))
                return cal.int2date(df.iloc[0].name.astype('int64'))
        else:
            return None

    def get_attribute(self):
        return self.store.attribute(StockBarLoader.get_freq_key(self.freq))

    def save(self, symbol, df, _append=True):
        if df is not None:
            key = StockBarLoader.get_freq_key(self.freq)
            self.store.save(key, df, _append=_append, min_itemsize={'trade_status': 16})
            self.store.attribute(key, **{'update': cal.date2int(cal.now())})

    def __refresh_min(self, duration):
        # TODO
        return None

    def __refresh_day(self, start, end):
        append = True
        last = self.get_store_last_date()
        attr_update = self.get_attribute().get('update')
        if attr_update is not None and attr_update >= cal.date2int(cal.now()):
            return
        if last is None or last < start:
            append = False
        elif end > last:
            start = last + datetime.timedelta(days=1)
        else:
            return
        logger.info("request daily from %s to %s" %
                    (start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")))
        daily = StockBarLoader.__api.daily(self.symbol, _start_date=start,
                                           _end_date=end, _freq=FREQ_DAY)
        if daily.empty is True:
            raise Exception("get empty")
        else:
            self.save(self.symbol, daily.raw(), _append=append)

    def __refresh_week(self, start, end):
        last = self.get_store_last_date()
        append = True
        if last is None or last < start:
            append = False
        elif end > last:
            start = last + datetime.timedelta(days=7)
        else:
            # newest
            return
        logger.info("request weekly from %s to %s" %
                    (start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")))
        daily = StockBarLoader.__api.daily(self.symbol, _start_date=start,
                                           _end_date=end, _freq=FREQ_WEEK)
        if daily.empty is True:
            raise Exception("get empty")
        else:
            self.save(self.symbol, daily.raw(), _append=append)

    def refresh(self, start=cal.now(),  duration=1440, freq=FREQ_DAY):
        self.freq = freq
        now = cal.date2string(start)
        if freq == FREQ_1M or freq == FREQ_5M or freq == FREQ_15M:
            self.__refresh_min(duration)
        elif freq == FREQ_DAY:
            start, end = cal.duration(_date=now, _days=-duration)
            self.__refresh_day(start, end)
        elif freq == FREQ_WEEK:
            start, end = cal.duration(_date=now, _days=-duration * 7)
            self.__refresh_week(start, end)

    def __load(self, where, freq):
        self.set(self.symbol)
        self.refresh()
        key = self.get_freq_key(freq)
        return self.store.select(key, where=where)

    def load(self, _start, _end, _freq):
        if _freq == FREQ_DAY or _freq == FREQ_WEEK or _freq == FREQ_MONTH:
            start = cal.date2int(_start)
            end = cal.date2int(_end)
            where = 'index >= %d & index <= %d' % (start, end)
            return self.__load(where=where, freq=_freq)
        else:
            # TODO
            pass
        pass


class StockBar(object):
    @staticmethod
    def create(symbol, freq, df):
        return StockBar(symbol, freq, df)

    @staticmethod
    def from_hdf(filename):
        key = 'data'
        store = pd.HDFStore(filename, format='table')
        df = store.get(key)
        info = store.get_storer(key).attrs.update
        return StockBar.create(info['symbol'], info['freq'], df)

    def __init__(self, symbol, freq, df):
        self.symbol = symbol
        self.freq = freq
        self.df = df

    def MA(self, *args):
        ret = []
        close = self.df['close'].values
        for i, val in enumerate(*args):
            if 'MA' + str(val) in self.df.keys():
                ret.append(self.df['MA' + str(val)])
            else:
                real = talib.MA(close, timeperiod=val)
                self.df['MA' + str(val)] = real
                ret.append(real)
        return ret

    def volume(self):
        return self.df['volume']

    def macd(self):
        ret = []
        keys = self.df.keys()
        if 'dif' in keys and 'dea' in keys and 'macd' in keys:
            return [self.df['dif'], self.df['dea'], self.df['macd']]
        else:
            close = self.df['close'].values
            self.df['dif'], self.df['dea'], self.df['macd'] = talib.MACD(
                close, fastperiod=12, slowperiod=26, signalperiod=9)
            return self.df['dif'], self.df['dea'], self.df['macd']

    def duration(self):
        if self.df is None:
            return None, None
        if self.df.empty is True:
            return None, None
        return (cal.int2date(self.df.iloc[0].name.astype('int64')),
                cal.int2date(self.df.iloc[-1].name.astype('int64')))

    def oclh(self):
        return self.df[['open', 'close', 'low', 'high']]

    def date(self):
        return self.df.index.values

    def to_hdf(self, filename):
        key = 'data'
        if not self.df.empty:
            store = pd.HDFStore(filename, format='table')
            store.put(key, self.df, format='table', data_columns=True)
            store.get_storer(key).attrs['update'] = {'symbol': self.symbol, 'freq': self.freq}
            store.close()


class StockCore(object):
    __api = Api()
    __composite = Composite()

    def __init__(self, symbol):
        self.symbol = symbol
        self.info = StockCore.__composite.get_info(symbol)
        self.bar = StockBarLoader()
        self.bar.set(symbol)

    def __del__(self):
        del self.bar

    def release(self):
        self.bar.release()

    def get_info(self):
        return self.info

    @property
    def suspension(self):
        now = cal.day()
        df = self.bar.load(now, now, FREQ_DAY)
        if df.empty:
            return True
        elif df.iloc[0]['volume'] == 0:
            return True
        else:
            return False

    def loadbar(self, _start, _end, _freq=FREQ_DAY):
        start = end = cal.day()
        if isinstance(_start, str) and isinstance(_end, str):
            if start != "" and end != "":
                start = cal.string2date(_start)
                end = cal.string2date(_end)
        elif isinstance(_start, int) and isinstance(_end, int):
            start = cal.int2date(_start)
            end = cal.int2date(_end)
        else:
            start = _start
            end = _end
        df = self.bar.load(start, end, _freq)
        return StockBar.create(self.symbol, _freq, df)


@singleton
class StockFactory(object):

    __stock_dict = {}
    __released_stock_list = []

    def __init__(self):
        pass
    # must have 'symbols' key
    #@classmethod
    # def stocks(cls, extraDataFrame):
    #    stocks = []
    #    if 'symbol' in extraDataFrame.raw().keys():
    #        symbols = extraDataFrame('symbol')
    #        for symbol in symbols:
    #            form = {extraDataFrame.__class__.__name__ : extraDataFrame.query('symbol', symbol)}
    #            stock = cls.stock(symbol, **form)
    #            if stock is not None:
    #                stocks.append(stock)
    #        return stocks
    #    else:
    #        raise Exception("symbols columns isn't exist")
    #    return stocks

    def recover(self, stock):
        if stock.symbol in self.__stock_dict:
            self.__stock_dict.pop(stock.symbol)
        self.__released_stock_list.append(stock)

    def stock(self, symbol):
        s = self.__stock_dict.get(symbol, None)
        if s is None:
            if len(self.__released_stock_list) > 0:
                s = self.__released_stock_list.pop()
                s.reload(symbol)
            else:
                s = Stock(symbol)
            self.__stock_dict[symbol] = s
        return s


class Stock(object):
    __api = Api()

    def __init__(self, symbol):
        self.symbol = symbol
        self.core = StockCore(symbol)
        self.bars = {}

    def reload(self, symbol):
        self.symbol = symbol
        self.core.release()
        self.core = StockCore(symbol)
        self.bars = {}

    def __del__(self):
        del self.core

    def release(self):
        self.core.release()
        StockFactory().recover(self)

    @property
    def name(self):
        return self.core.get_info()['name']

    def __str__(self):
        pass

    def dump(self):
        pass

    def isnew(self):
        return (cal.date2int(cal.now())) - self.core.get_info()['list_date'] < 30

    def __contain(self, b, start, end):
        sync_start = cal.date2int(start)
        sync_end = cal.date2int(end)
        store_start, store_end = b.duration()
        if store_start is None or store_end is None:
            return False
        else:
            return store_start <= start and store_end >= end

    def bar(self, start, end, freq=FREQ_DAY):
        b = self.bars.get(freq)
        if b is None or not self.__contain(b, start, end):
            b = self.core.loadbar(_start=start, _end=end,
                                  _freq=freq)
            self.bars[freq] = b
        return b


if __name__ == "__main__":
    s = Stock('000001.SZ')
    print(s.isnew())
