# -*- coding: utf-8 -*-
import os
import talib
import numpy as np
import pandas as pd
from .api import TradeCalendar as cal, singleton, Api, DailyDataFrame, logger, JsonLoader, StockStore, LOCAL_CACHE_PATH, LOCAL_STORE_PATH, logger
from .composite import Composite

FREQ_1M = '1M'
FREQ_5M = '5M'
FREQ_15M = '15M'
FREQ_DAY = '1d'
FREQ_WEEK = '1w'
FREQ_MONTH = '1m'



class StockBarLoader(object):
    __api = Api()

    __CACHE_CONFIG = os.path.join(LOCAL_STORE_PATH, 'cache.json')
    if not os.path.exists(__CACHE_CONFIG):
        JsonLoader.create(__CACHE_CONFIG)

    @classmethod
    def get_sync_date(cls):
        l = JsonLoader.create(cls.__CACHE_CONFIG)
        date = l.get('date')
        return None if date is None else cal.create(date)

    @classmethod
    def update_sync_date(cls):
        l = JsonLoader.create(cls.__CACHE_CONFIG)
        l.put(**{'date': cal.now().to_str()})

    @staticmethod
    def sync_stock_bar(**args):
        now = cal.now()
        symbols = Composite().get_all_symbols()
        last = StockBarLoader.get_sync_date()
        if last is None or (now - last).days > 0:
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


    @staticmethod
    def sync_checker():
        ## read cache date
        date = StockBarLoader.get_sync_date()
        symbols = Composite().get_all_symbols()
        c = StockBarLoader()
        for symbol in symbols:
            c.set(symbol)
            start, end = c.get_store_date_duraion(FREQ_DAY)
            if end is None:
                logger.error("checker %s end date is None" % symbol)
            elif (end - date).days != 0:
                logger.error("checker %s end date is %s" % (symbol, end.to_str()))




    @classmethod
    def get_store(cls, symbol):
        return StockStore(symbol)

    @classmethod
    def get_freq_key(self, freq):
        return '/freq/' + 'F' + freq

    def __init__(self):
        self.symbol = ""
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

    def get_store_date_duraion(self, freq):
        key = StockBarLoader.get_freq_key(freq)
        if key in self.store.keys() and self.store.get(key) is not None:
            row = len(self.store.get(key).index)
            row_b = self.store.select(key, start=0, end=1)
            row_l = self.store.select(key, start=row - 1, end=row)
            if row_b.empty or row_l.empty:
                logger.debug('store is empty, request all')
                return None, None
            else:
                if row_l.iloc[0][DailyDataFrame.TRADE_STATUS] == DailyDataFrame.TRADE_SUSPENSION:
                    logger.debug('%s [%s]' % (self.symbol, DailyDataFrame.TRADE_SUSPENSION))
                return cal.create(row_b.iloc[0].name.astype('int64')), cal.create(row_l.iloc[0].name.astype('int64'))
        else:
            return None, None

    def get_attribute(self, freq):
        return self.store.attribute(StockBarLoader.get_freq_key(freq))


    def save(self, symbol, freq, df, _append=True):
        if df is not None:
            key = StockBarLoader.get_freq_key(freq)
            self.store.save(key, df, _append=_append, min_itemsize={'trade_status': 16})
            self.store.attribute(key, **{'update': cal.now().to_int()})

    def __refresh_min(self, duration):
        # TODO
        return None

    def __refresh_day(self, start, end):
        append = True
        freq = FREQ_DAY
        begin, last = self.get_store_date_duraion(freq)
        attr_update = self.get_attribute(freq).get('update')
        if attr_update is not None and attr_update >= cal.now().to_int():
            return
        if begin is None or (begin - start).days > 0:
            append = False
        elif (end - last).days > 0:
            start = last.shift(1)
        else:
            return
        logger.info("request daily from %s to %s" %
                    (start.to_str(), end.to_str()))
        daily = StockBarLoader.__api.daily(self.symbol, _start_date=start,
                                           _end_date=end, _freq=freq)
        if daily.empty is False:
            self.save(self.symbol, freq, daily, _append=append)
        else:
            logger.warn("df is empty")

    def __refresh_week(self, start, end):
        freq = FREQ_WEEK
        append = True
        begin, last = self.get_store_date_duraion(freq)
        if last is None or last < start:
            append = False
        elif end > last:
            next_week = last.shift(7)
            if next_week <= end:
                start = next_week
            else:
                ## isn't generate this week
                return
        else:
            # newest
            return
        logger.info("request weekly from %s to %s" %
                    (start.to_str(), end.to_str()))
        weekly = StockBarLoader.__api.daily(self.symbol, _start_date=start,
                                           _end_date=end, _freq=freq)
        if weekly.empty is False:
            self.save(self.symbol, freq, weekly, _append=append)
        else:
            logger.warn("df is empty")

    def refresh(self, start=cal.now(),  duration=1440, freq=FREQ_DAY):
        if freq == FREQ_1M or freq == FREQ_5M or freq == FREQ_15M:
            self.__refresh_min(duration)
        elif freq == FREQ_DAY:
            start, end = cal.duration(_date=start, _days=-duration)
            self.__refresh_day(start, end)
        elif freq == FREQ_WEEK:
            start, end = cal.duration(_date=start, _days=-duration * 7)
            self.__refresh_week(start, end)

    def __load(self, where, freq):
        key = self.get_freq_key(freq)
        self.set(self.symbol)
        self.refresh()
        if key in self.store.keys():
            return self.store.select(key, where=where)
        else:
            logger.error("[%s] have no %s bar in store" % (self.symbol, freq))
            return None

    def load(self, _start, _end, _freq):
        if _freq == FREQ_DAY or _freq == FREQ_WEEK or _freq == FREQ_MONTH:
            start = _start.to_int()
            end = _end.to_int()
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
        if self.df.empty:
            return None, None
        return (cal.create(self.df.iloc[0].name.astype('int64')),
                cal.create(self.df.iloc[-1].name.astype('int64')))

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
        start = end = cal.now()
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
        if df is not None and df.empty == False:
            return StockBar.create(self.symbol, _freq, df)
        else:
            return None


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
        timedelta = cal.now() -  cal.create(self.core.get_info()['list_date'].astype('int64'))
        return timedelta.days < 30

    def __contain(self, b, start, end):
        store_start, store_end = b.duration()
        if store_start is None or store_end is None:
            return False
        else:
            return store_start <= start and store_end >= end

    def bar(self, start, end, freq=FREQ_DAY):
        b = self.bars.get(freq)
        _start = cal.create(start)
        _end = cal.create(end)
        if b is None or not self.__contain(b, _start, _end):
            b = self.core.loadbar(_start=start, _end=end,
                                  _freq=freq)
            self.bars[freq] = b
        return b


if __name__ == "__main__":
    pass
    #s = Stock('000001.SZ')
    #print(s.isnew())
