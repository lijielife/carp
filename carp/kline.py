# -*- coding: utf-8 -*-
import os
import datetime
import pandas as pd
from carp.util import log
from carp import db
from carp import request
from carp import util
from carp import trade_calendar
from carp.config import GlobalConfig as CONFIG



class KlineQuery(object):

    def __init__(self, symbol):
        self.symbol = symbol

    def isEmpty(self, freq):
        helper = KlinedbHelper.load_db_helper(freq)
        return not helper.exist(self.symbol)

    def duration(self, freq):
        helper = KlinedbHelper.load_db_helper(freq)
        start, end = helper.duration(self.symbol)
        return trade_calendar.create_v2(start, freq), trade_calendar.create_v2(end, freq)

    def load(self, start, end, freq, ascending, limit = 0):
        helper = KlinedbHelper.load_db_helper(freq)
        if start is not None:
            start = trade_calendar.create_v2(start, freq)
        if end is not None:
            end = trade_calendar.create_v2(end, freq)
        df = helper.load(self.symbol, start, end, limit = limit);
        if isinstance(df, pd.DataFrame) and df.empty == False:
            df = df.sort_values(by = 'datetime', ascending = ascending)
            return df.reset_index(drop = True)
        else:
            return None



class KlinedbHelper(object):

    __DATABASE_NAME = 'kline'
    __COLLECTION_NAME_FORMAT = 'kline_freq_{}'

    APPEND = 1
    REPLACE = 2
    NONE = 3

    __FREQ_COLLECTION_ = {}

    @staticmethod
    def load_db_helper(freq):
        return KlinedbHelper(freq)

    def __init__(self, freq):
        self.freq = freq
        self.__mongodb = db.create_db(KlinedbHelper.__DATABASE_NAME)
        self.table = self.__mongodb.get_table(KlinedbHelper.__COLLECTION_NAME_FORMAT.format(freq))

    def duration(self, symbol):
        cursor = self.table.find_one({'_id': symbol})
        if cursor is None or cursor.get('info') is None: # symbol isn't existing
            return (None, None)
        else:
            return cursor['info']['start_date'], cursor['info']['end_date']


    def __save(self, symbol, start, end, df, opt):
        if opt == self.REPLACE:
            self.__replace(symbol, start, end, df)
        elif opt == self.APPEND:
            self.__append(symbol, start, end, df)

    def __append(self, symbol, start, end, df):
        log.info('append %s from %s to %s'% (symbol, start, end))
        klines = util.to_json(df)
        for k in klines:
            k['datetime'] = trade_calendar.create_v2(k['datetime'], self.freq).datetime()

        cursor = self.table.update({'_id': symbol}, {
            '$set':{'info.end_date': end.datetime()}, \
            ## TODO waiting for $position
            '$push': {'kline': {'$each':klines}}}, upsert=True)

    def __replace(self, symbol, start, end, df):
        log.info('replace %s from %s to %s'% (symbol, start, end))
        info = {'symbol': symbol, 'start_date':start.datetime(),\
                'end_date':end.datetime()}
        klines = util.to_json(df)
        for k in klines:
            k['datetime'] = trade_calendar.create_v2(k['datetime'], self.freq).datetime()

        self.table.update({'_id': symbol}, {
            '$set':{'info': info}, \
            ## TODO sort operation
            '$setOnInsert':{'kline': klines} \
            }, upsert=True)
        log.debug('replace end')


    def __append_or_replace(self, src, des):
        opt = self.NONE
        START = 0
        END = 1
        src = list(map(lambda x: trade_calendar.create_v2(x, self.freq), src))
        des = list(map(lambda x: trade_calendar.create_v2(x, self.freq), des))
        if src[START] is None or src[END] is None or (des[START] < src[START] and des[END] > des[END]):
            opt = self.REPLACE
        elif des[START] is None or des[END] is None or (des[START] > src[START] and des[END] < src[END]):
            opt = self.NONE
        elif des[START] < src[START] and des[END] > src[START]:
            ## TODO have no solution to do
            opt = self.NONE
        elif des[START] > src[END]:
            opt = self.APPEND
        return opt


    def save(self, symbol, start, end, df):
        if df is None or not isinstance(df, pd.DataFrame):
            return -1

        if not self.exist(symbol):
            self.__save(symbol, start, end, df, self.REPLACE)
            return 0
        opt = self.__append_or_replace(self.duration(symbol), [start, end])

        if opt != self.NONE:
            self.__save(symbol, start, end, df, opt)

    def exist(self, symbol):
        return self.cursor(symbol) is not None

    def cursor(self, symbol):
        return self.table.find_one({'_id': symbol})

    def load(self, symbol, start, end, limit = 0):
        condition = []
        condition.append({'$match':{'_id':symbol}})
        condition.append({'$project':{'kline':1}})
        condition.append({'$unwind':'$kline'})
        condition.append({'$sort': {'kline.datetime': -1}})
        if limit == 0 or (start is not None and end is not None):
            condition.append({'$match': {'kline.datetime': {'$gte':start.datetime(), '$lte':end.datetime()}}})
        elif end is not None:
            condition.append({'$match': {'kline.datetime': {'$lte':end.datetime()}}})
            condition.append({'$limit': limit})
        else:
            condition.append({'$match': {'kline.datetime': {'$gte':start.datetime()}}})
            condition.append({'$limit': limit})

        condition.append({'$group': {"_id": '$_id', 'kline': {'$push': '$kline'}}})
        cursor = list(self.table.aggregate(condition))
        if len(cursor) == 0:
            return None
        if 'kline' in cursor[0]:
            return pd.DataFrame(cursor[0].get('kline'))
        else:
            raise RuntimeError('%s' % cursor[0])


    def clear(self):
        self.table.drop()




def append_kline(symbol, start_date, end_date, freq, df):
    helper = KlinedbHelper.load_db_helper(freq)
    ## TODO add more check
    helper.save(symbol, trade_calendar.create_v2(start_date, freq),
            trade_calendar.create_v2(end_date, freq), df)



def set_sync_result(result, freq, date):
    db = KlinedbHelper.load_db_helper(freq)
    db.table.update({'_id': 'info'}, {'$set':{'result': result, 'date':date.datetime()}}, upsert=True)


def get_sync_result(freq):
    db = KlinedbHelper.load_db_helper(freq)
    cursor = db.table.find_one({'_id': 'info'})
    return cursor

