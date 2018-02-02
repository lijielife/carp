# -*- coding: utf-8 -*-
import os
import functools
import datetime
import arrow
import copy
import pandas as pd
import numpy as np
from carp.util import log
from carp import util
from carp import db


class CalendarReader(object):
    __filename = os.path.join(os.getcwd(), 'calendar.h5')

    __key_map = {
            util.FREQ_WEEK : 'FREQ_WEEK',
            util.FREQ_DAY : 'FREQ_DAY',
            util.FREQ_30M: 'FREQ_30MIN',
            util.FREQ_15M: 'FREQ_15MIN',
            util.FREQ_5M: 'FREQ_5MIN',
            util.FREQ_1M: 'FREQ_1MIN',
            }

    def __init__(self, force = False):
        self.__single_date_cache = {}
        self.__duration_date_cached = []

        if os.path.exists(self.__filename):
            self.__store = db.H5Store.create(CalendarReader.__filename, force)
        else:
            raise RuntimeError('%s isn\'t exist'% self.__filename)

    @classmethod
    def freq2key(self, freq):
        if freq in CalendarReader.__key_map:
            return '/' + CalendarReader.__key_map[freq]
        else:
            raise KeyError('no %s in __key_map' % freq)


    def __query_where(self, key, start, end, method):
        where = ''
        border = {
                'left': ['>=', '<'],
                'right': ['>', '<='],
                'all':['>=', '<='],
                'none':['>', '<']
                }

        connect = ' ' if start is None or end is None else '& '
        if start is not None:
            where = 'date {} \'{}\''.format(border[method][0], start)

        where += connect
        if end is not None:
            where = 'date {} \'{}\''.format(border[method][1], end)

        df = self.__store.select(key, where = where)
        if not isinstance(df, pd.DataFrame):
            raise RuntimeError('select error')

        if df.empty:
            log.warn('query date empry')
            return []
        else:
            return df


    def __query_row(self, key, start, end):
        df = self.__store.select(key, start = start, stop = end)
        if df.empty:
            log.warn('query date empry')
            return []
        else:
            return df


    def get_last(self, freq, end):
        if not freq in self.__single_date_cache:
            self.__single_date_cache[freq] = {}

        if end in self.__single_date_cache[freq]:
            return self.__single_date_cache[freq][end]

        df = self.query(freq, start = None, end = end)
        if df.empty:
            raise RuntimeError('last is None')
        self.__single_date_cache[freq][end] = df.iloc[-1]
        return df.iloc[-1]


    ## left/right/all/none
    def query(self, freq, start, end, method = 'all'):
        key = self.freq2key(freq)
        if key not in self.__store.keys():
            raise KeyError('no %s in h5' % key)

        if isinstance(start, str) or isinstance(end, str):
            return self.__query_where(key, start, end, method)
        elif isinstance(start, int) and isinstance(end, int):
            return self.__query_row(key, start, end)
        else:
            raise TypeError('start & end type error')



class TradeCalendarV2(object):

    __READER = None

    @classmethod
    def initialze_reader(cls, force = False):
        if force == True:
            cls.__READER = CalendarReader(force)
        else:
            if cls.__READER is None:
                cls.__READER = CalendarReader(force)
        return cls.__READER


    @classmethod
    def format_str_freq(cls, freq):
        if freq == util.FREQ_1M or \
                freq == util.FREQ_5M or \
                freq == util.FREQ_15M or \
                freq == util.FREQ_30M:
            return 'YYYY-MM-DD HH:mm:ss'
        else:
            return 'YYYY-MM-DD'


    @classmethod
    def __format_date(cls, date, freq):
        return date.format(cls.format_str_freq(freq))

    def __init__(self, date, freq, adjust):
        self.__freq = freq
        if adjust:
            date_str = self.__format_date(date, freq)
            self.__reader = TradeCalendarV2.initialze_reader(False)
            series = self.__reader.get_last(freq, date_str)
            self.__datetime = arrow.get(pd.to_datetime(series.values[0]))
        else:
            self.__datetime = arrow.get(copy.deepcopy(date))


    def shift(self, count):
        find_str = self.format()
        if count == 0:
            return self

        if count > 0:
            df = self.__reader.query(self.__freq, start = find_str, end = None)
        elif count < 0:
            df = self.__reader.query(self.__freq, start = None, end = find_str, method = 'left')

        if df.empty:
            raise RuntimeError('out of range')
        else:
            self.__datetime = arrow.get(pd.to_datetime(df.iloc[count].values[0]))
        return self

    def format(self):
        return self.__datetime.format(self.format_str_freq(self.__freq))

    def index(self):
        i = self.format()
        series = self.__reader.get_last(self.__freq, i)
        return series.name

    def __str__(self):
        return self.__datetime.__str__()

    def __repr__(self):
        return self.__datetime.__repr__()

    def datetime(self):
        return self.__datetime.datetime

    def __ne__(self, other):
        return self.__datetime.__ne__(other.__datetime)

    def __eq__(self, other):
        return self.__datetime.__eq__(other.__datetime)

    def __gt__(self, other):
        return self.__datetime.__gt__(other.__datetime)

    def __ge__(self, other):
        return self.__datetime.__ge__(other.__datetime)

    def __lt__(self, other):
        return self.__datetime.__lt__(other.__datetime)

    def __le__(self, other):
        return self.__datetime.__le__(other.__datetime)


def last_trade_date(freq):
    now = arrow.now()
    r = create_v2(now, freq = freq)
    if freq == util.FREQ_DAY:
        v = r.datetime()
        if v.date() == now.datetime.date() and now.hour <= 16:
            return r.shift(-1)
        else:
            return r
    ## TODO
    else:
        return r



def create_v2(date, freq):
    adjust = False if freq == util.FREQ_MONTH else True
    _arrow = None
    if isinstance(date, str):
        format_str = TradeCalendarV2.format_str_freq(freq)
        _arrow = arrow.get(date, format_str)
    elif isinstance(date, datetime.datetime):
        _arrow = arrow.get(date)
    elif isinstance(date, TradeCalendarV2):
        _arrow = arrow.get(date.datetime())
    elif isinstance(date, arrow.Arrow):
        _arrow = date
    else:
        raise TypeError('known type %s' % type(date))
    return TradeCalendarV2(_arrow, freq = freq, adjust = adjust)


def schedule_v2(start_date, end_date, freq):
    raise NotImplementedError('not implemented')


def compare_v2(date1, date2, freq):
    d1 = create_v2(date1, freq)
    d2 = create_v2(date2, freq)
    return d1.index() - d2.index()




#class TradeCalendar(object):
#    __filename = os.path.join(os.getcwd(), 'calendar.csv')
#    __calendar = None
#
#    @classmethod
#    def __load_trade_cal(cls):
#        if cls.__calendar is None:
#            df = pd.read_csv(cls.__filename)
#            df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d', errors='raise')
#            #df.set_index('date', inplace = True)
#            cls.__calendar = df
#        return cls.__calendar
#
#    @classmethod
#    def get_cal_df(cls, **kwargs):
#        df = cls.__load_trade_cal()
#        name = 'date'
#        if 'start_date' in kwargs:
#            df = df[df[name] >= kwargs['start_date']]
#
#        if 'end_date' in kwargs:
#            df = df[df[name] <= kwargs['end_date']]
#
#        if 'offset' in kwargs:
#            offset =  kwargs['offset']
#            return df[offset:] if offset > 0 else df[:offset]
#        return df
#
#
#
#    def __init__(self, date, trade = True):
#        self.__trade = trade
#        if self.__trade:
#            dates = self.get_cal_df(end_date=date.format('YYYY-MM-DD'))
#            if dates.size > 0:
#                self.__arrow = arrow.get(dates['date'].iloc[-1].to_pydatetime())
#            else:
#                original = arrow.get(self.get_cal_df().iloc[0]['date'])
#                if original > date:
#                   log.warn('%s is beyond original, use %s instead' % (date.format('YYYY-MM-DD'), original.format('YYYY-MM-DD')))
#                   self.__arrow = original
#                else:
#                    raise Exception("date is unknown")
#        else:
#            self.__arrow = date
#
#    def __add__(self, other):
#        return self.__arrow.__add__(other.__arrow)
#
#    def __sub__(self, other):
#        return self.__arrow.__sub__(other.__arrow)
#
#    def __eq__(self, other):
#        return self.__arrow.__eq__(other.__arrow)
#
#    def __ne__(self, other):
#        return self.__arrow.__ne__(other.__arrow)
#
#    def __gt__(self, other):
#        return self.__arrow.__gt__(other.__arrow)
#
#    def __ge__(self, other):
#        return self.__arrow.__ge__(other.__arrow)
#
#    def __lt__(self, other):
#        return self.__arrow.__lt__(other.__arrow)
#
#    def __le__(self, other):
#        return self.__arrow.__le__(other.__arrow)
#
#    def __str__(self):
#        return self.__arrow.__str__()
#
#    def __repr__(self):
#        return self.__arrow.__repr__()
#
#
#    ##day
#    def shift(self, count=1,  freq = 'D'):
#        if self.__trade:
#            ## TODO
#            if freq == 'D':
#                df = TradeCalendar.get_cal_df(start_date = self.__arrow.format('YYYY-MM-DD'),
#                        offset=count)
#                self.__arrow = arrow.get(df['date'].iloc[0].to_pydatetime())
#            elif freq == '30M':
#                pass
#        else:
#            ## TODO
#            _arrow = self.__arrow.shift(days = count)
#        return self
#        #return TradeCalendar(_arrow, trade = self.__trade)
#
#    #def to_int(self):
#    #    year = self.__arrow.year
#    #    month = self.__arrow.month
#    #    day = self.__arrow.day
#    #    return year * 10000 + month * 100 + day
#
#    def format(self):
#        ## TODO add freq
#        return self.__arrow.format('YYYY-MM-DD')
#
#    def get(self):
#        return self.__arrow
#
#    def get_datetime(self):
#        ## TODO add freq
#        return self.__arrow.datetime
#
#
#def type2arrow(v):
#    if isinstance(v, str):
#        ret = arrow.get(v, 'YYYY-MM-DD')
#    elif isinstance(v, datetime.datetime):
#        ret = arrow.get(v)
#    elif isinstance(v, arrow.Arrow):
#        ret = v
#    else:
#        raise TypeError('known type %s' % type(v))
#    return ret
#
#def create(v, trade = True):
#    if isinstance(v, TradeCalendar):
#        return v
#    else:
#       date = type2arrow(v)
#       return TradeCalendar(date, trade = trade)
#
#
#def now(trade = True):
#    n = arrow.now()
#    return create(n, trade = trade)
#

#def less(date1, date2, freq = 'D'):
#    return compare(date1, date2, freq) < 0
#
#def compare_count(date1, date2, freq = 'Day', count = 1):
#    c1 = create(date1)
#    c2 = create(date2)
#    interval = (c1.get() - c2.get())
#    ret = 0
#    if freq == 'Day':
#        ret = interval.days
#    elif freq == 'Min':
#        ## FIXME
#        ret = interval.minutes
#    elif freq == 'Week':
#        ret = interval.days / 7
#    ## TODO
#    else:
#        raise ValueError('unsupport type %s' % freq)
#    return ret / count
#
#def compare(date1, date2, freq):
#    if freq == util.FREQ_DAY:
#        return compare_count(date1, date2, freq = 'Day')
#    elif freq == util.FREQ_WEEK:
#        return compare_count(date1, date2, freq = 'Week')
#    elif freq == util.FREQ_30M:
#        return compare_count(date1, date2, freq = 'Min', count = 30)
#    else:
#        raise ValueError('unsupport type %s' % freq)
#

#def schedule(start_date, end_date, freq = 'D'):
#    datetime_df = TradeCalendar.get_cal_df(start_date = start_date, end_date = end_date)
#    if datetime_df.size <= 0:
#        raise Exception('schedule error')
#
#    df = datetime_df.copy()
#    df.set_index('date', inplace = True)
#
#    if freq == 'D':
#        return df.index
#    elif freq == '30M':
#        df30min = df.asfreq('30Min', 'ffill')
#        x1 = df30min.ix[df30min.index.indexer_between_time(datetime.time(9, 30), datetime.time(11, 30))]
#        x2 = df30min.ix[df30min.index.indexer_between_time(datetime.time(13), datetime.time(15))]
#        return pd.concat([x1, x2]).sort_index().index
#    # TODO
#

