# -*- coding: utf-8 -*-
import os
import datetime
import multiprocessing
import pandas as pd
import numpy as np
import arrow
import functools
from carp import config
from carp import util
from carp.util import log
from carp import request
from carp import composite
from carp import kline
from carp import db
from carp import trade_calendar

#carp_task = celery.Celery('tasks', broker=config.GlobalConfig.CELERY_REDIS_BROKER_URL ,\
#        backend=config.GlobalConfig.CELERY_REDIS_BACKEND_URL)


def day_sync_time(name):
    now = arrow.now()
    r = trade_calendar.create_v2(now, freq = util.FREQ_DAY)
    v = r.datetime()
    if v.date() == now.datetime.date() and now.hour <= 16:
        log.error('request %s need after 16:00:00' % name)
        return False
    else:
        return True

class TodayAllDailyKline(object):

    # FIXME use hdf5 intead

    @classmethod
    def has_cache(cls, date):
        filename = os.path.join(config.GlobalConfig.CACHE_PATH, '%s.csv' % date.format())
        return os.path.exists(filename)

    def __init__(self, date):
        self.__date = date
        self.__filename = os.path.join(config.GlobalConfig.CACHE_PATH, '%s.csv' % date.format())
        self.__df = None


    def exist(self, symbol):
        df = self.load()
        if df is not None:
            return symbol in df['code'].values
        else:
            return False

    def get(self, symbol):
        df = self.load()
        ret = df[df['code'] == symbol]
        log.debug('symbol %s' % symbol)
        if isinstance(ret, pd.DataFrame):
            return ret[['close', 'open', 'high', 'low', 'vol', 'tor', 'amount', 'datetime']]
        else:
            raise RuntimeError('invalid symbol %s' % symbol)

    def trade(self, symbol):
        df = self.get(symbol)
        if df.empty:
            return None
        zero = lambda x : df[x].iloc[0]
        if zero('close') != 0 or zero('open') != 0 or zero('high') != 0 or \
                zero('low') != 0 or zero('vol') != 0 or zero('tor') != 0 or zero('amount') != 0:
            return df
        else:
            return None


    def __adjust(self, df):
            df['code'] = df['code'].astype(str).str.zfill(6)
            df.drop(['name', 'changepercent', 'settlement', 'per', \
                    'pb', 'mktcap', 'nmc'], axis = 1, inplace = True)
            df.rename(columns = {'volume':'vol', 'trade':'close', 'turnoverratio': 'tor'}, inplace = True)
            df['vol'] = df['vol'] / 100
            df['datetime'] = self.__date.format()

    def load(self):
        if self.__df is None and TodayAllDailyKline.has_cache(self.__date):
            self.__df = pd.read_csv(self.__filename)
            self.__adjust(self.__df)
        return self.__df

    def catch(self, df):
        if isinstance(df, pd.DataFrame) and df.empty == False:
            self.__df = df
            self.__df.to_csv(self.__filename)
            self.__adjust(self.__df)
        return self.__df



#@carp_task.task()
def request_stock_basic(saveDB = True, **kwargs):
    basic = composite.get_basic()

    if basic.newest():
        log.info('stocks list needn\'t update')
        return basic

    if day_sync_time('request_stock_basic'):
        log.info('request stock basic')
        df = request.request_stock_basic()
        composite.update_stocks(df)
        return basic
    else:
        return None



#@carp_task.task()
def request_kline(symbol, freq , **kwargs):
    query = kline.KlineQuery(symbol)
    log.debug('begin to request [%s]' % symbol)
    if query.isEmpty(freq):
        ## TODO request all
        info = composite.get_basic().info(symbol)
        if info is None:
            raise ValueError('%s not exist'% symbol)

        born = info['timeToMarket']
        return request_kline_core(symbol, start_date=trade_calendar.create_v2(born, freq), end_date = trade_calendar.last_trade_date(freq), freq = freq, **kwargs)
    else:
        db_start, db_end = query.duration(freq)
        if db_start is None and db_end is None:
            log.error('duration in db is error')
            raise RuntimeError('[%s] isn\'t exist, but duration ???' % symbol)

        last_date = trade_calendar.last_trade_date(freq)

        if trade_calendar.compare_v2(db_end, last_date, freq) == 0:
            ## TODO check suspend stock
            log.info('%s needn\'t update' % symbol)
            return 1
        elif trade_calendar.compare_v2(db_end, last_date, freq) < 0:
            db_end.shift(count = 1)
            return request_kline_core(symbol, db_end, last_date, freq, **kwargs)
        else:
            raise RuntimeError("????")



#@carp_task.task()
def request_all_today_kline():
    df = None
    last_date = trade_calendar.last_trade_date(freq = util.FREQ_DAY)
    if not TodayAllDailyKline.has_cache(last_date):
        if day_sync_time('today_all'):
            log.info('request today_all')
            df = request.get_today_all()
            daily = TodayAllDailyKline(last_date)
            daily.catch(df)
    else:
        daily = TodayAllDailyKline(last_date)
        df = daily.load()
    return df



#@carp_task.task()
def request_kline_core(symbol, start_date, end_date, freq = util.FREQ_DAY , **kwargs):
    ## check start < end
    if trade_calendar.compare_v2(start_date, end_date, freq) > 0:
        log.warn('invalid {} -> {}'.format(start_date, end_date))
        return -1
    else:
        df = request.request_stock_bar(symbol, start = start_date, end = end_date, freq = freq)
        if not df.empty:
            log.info('request %s [%s -> %s] success'% (symbol, start_date, end_date))
            kline.append_kline(symbol, start_date, end_date, freq, df)
        else:
            log.warn('request %s empty'% (symbol))
        return 0



def worker(freq, symbol):
    request_kline(symbol, freq = freq)

def request_symbols_kline(symbols, freq):
    def child_fork_init():
        trade_calendar.TradeCalendarV2.initialze_reader(True)
        db.MongoDB.open_or_create_client(True)

    ## TODO if symbols more than 50 try use multi process
    if len(symbols) > 50:
        db.MongoDB.release()
        pool = multiprocessing.Pool(initializer=child_fork_init)
        inner_worker = functools.partial(worker, freq)
        pool.map(inner_worker, symbols)
        pool.close()
        pool.join()
    else:
        for symbol in symbols:
            request_kline(symbol, freq = freq)
                # run here mean all sucess
    date = trade_calendar.last_trade_date(freq)
    kline.set_sync_result(True, freq, date)



def __sync_day_kline(symbols, start, end):
    freq = util.FREQ_DAY
    s = trade_calendar.create_v2(start, freq)
    e = trade_calendar.create_v2(end, freq)
    s.shift(1)
    success = True
    log.info("merge from %s -> %s" % (s, e))

    ## check we can use daily csv to merge
    daily_merge = True
    _iter = trade_calendar.create_v2(s, freq)
    while _iter <= e:
        if not TodayAllDailyKline.has_cache(_iter):
            daily_merge = False
            break
        else:
            _iter.shift(1)

    ## use daily merge
    if daily_merge:
        while trade_calendar.compare_v2(e, s, freq = freq) >= 0:
            if TodayAllDailyKline.has_cache(s):
                today = TodayAllDailyKline(s)
                for symbol in symbols:
                    k = today.trade(symbol)
                    if k is not None:
                        kline.append_kline(symbol, s, s, freq = util.FREQ_DAY, df = k)
                    else:
                        log.info('{} with no trade in {}'.format(symbol, s))
            else:
                raise RuntimeError('have no daily data %s' % s.format())
            s.shift(1)
        kline.set_sync_result(True, freq, e)
    else:
        request_symbols_kline(symbols, freq)
    log.info('no error')



#@carp_task.task()
def sync_history_bar(symbols, freq):
    ret = kline.get_sync_result(freq)
    end = trade_calendar.last_trade_date(freq)
    if ret is not None and ret.get('result', False) == True and  \
            ret.get('date') is not None and trade_calendar.compare_v2(ret.get('date'), end, freq) >= 0:
        log.info('need\'t update')
    else:
        if freq == util.FREQ_DAY:
            if ret is None or ret.get('date') is None:
                ## no date in db , request all data
                log.info("request symbol list")
                request_symbols_kline(symbols, freq)
            else:
                request_all_today_kline()
                __sync_day_kline(symbols, ret.get('date'), end)
        elif freq == util.FREQ_WEEK:
            now = datetime.datetime.now()
            if now.weekday() >= 5: ##  rest day
                request_symbols_kline(symbols, freq)
            else:
                log.info('request weekly data in rest day ignore freq week')
        else:
            ## TODO
            request_symbols_kline(symbols, freq)



#def change_kline_patch(freq):
#    helper = kline.KlinedbHelper.load_db_helper(freq)
#
#    for cursor in helper.table.find():
#        if cursor['_id'] == 'info':
#            print(cursor['_id'])
#            cursor['date'] = datetime.datetime.strptime(cursor['date'], '%Y-%m-%d')
#            helper.table.replace_one({'_id': 'info'}, { \
#                '_id': 'info', \
#                'date': cursor['date'], \
#                'result': True})
#
#
#
    #for cursor in helper.table.find():
    #    if 'info' in cursor:
    #        print(cursor['info'])
    #        for k in cursor['kline']:
    #            if 'vr' in k:
    #                del k['vr']
    #        helper.table.replace_one({'_id': cursor['_id']}, { \
    #            '_id': cursor['_id'], \
    #            'info': cursor['info'], \
    #            'kline': cursor['kline']})


